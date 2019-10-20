package raftkv

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"strconv"
	"sync"
	"time"
)

const Debug = 0

const (
	PUT int = iota
	GET
	APPEND
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType  int
	Key     string
	Value   string
	Cid     int64
	QueryID int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	commandRecord map[int64]int
	kvdb          map[string]string
	replyCh       map[string]chan CommonReply

	quitCh       chan struct{}
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

func (kv *KVServer) getQueryName(cid int64, queryID int) string {
	return strconv.FormatInt(cid, 10) + "_" + strconv.Itoa(queryID)
}

func (kv *KVServer) createReplyCh(queryName string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.replyCh[queryName]
	if !ok {
		kv.replyCh[queryName] = make(chan CommonReply, 1)
	}
}

func (kv *KVServer) getReplyCh(queryName string) chan CommonReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ret := kv.replyCh[queryName]
	return ret
}

func (kv *KVServer) del(queryName string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, ok := kv.replyCh[queryName]
	if ok {
		close(kv.replyCh[queryName])
		delete(kv.replyCh, queryName)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		OpType:  GET,
		Key:     args.Key,
		Cid:     args.Cid,
		QueryID: args.QueryID,
	}

	reply.WrongLeader = false
	reply.Err = ""
	reply.Value = ""

	queryName := kv.getQueryName(args.Cid, args.QueryID)

	_, _, isLeader := kv.rf.Start(op)
	kv.logger(fmt.Sprintf("start a get op with key : %v, queryName : %v", args.Key, queryName))
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "error : not a leader"
		kv.logger(fmt.Sprintf("get query(%v) kv server is not a leader", queryName))
		return
	}
	kv.createReplyCh(queryName)
	replyCh := kv.getReplyCh(queryName)

	select {
	case r := <-replyCh:
		if !r.Success {
			reply.Err = "error : " + r.Err
			kv.logger(fmt.Sprintf("get query(%v) ", queryName) + string(reply.Err) + " in kv server")
		} else {
			kv.logger(fmt.Sprintf("get query(%v) success, key : %v, value : %v", queryName, args.Key, r.Value))
			reply.Value = r.Value
		}
	case <-time.After(400 * time.Millisecond):
		kv.logger(fmt.Sprintf("get query(%v) time out in kv server", queryName))
		reply.Err = "error : timeout"
	}

	kv.del(queryName)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.logger(fmt.Sprintf("receives PutAppend Request op : %v, key : %v, value : %v", args.Op, args.Key, args.Value))

	op := Op{
		Key:     args.Key,
		Value:   args.Value,
		Cid:     args.Cid,
		QueryID: args.QueryID,
	}
	if args.Op == "Put" {
		op.OpType = PUT
	} else {
		op.OpType = APPEND
	}

	reply.WrongLeader = false
	reply.Err = ""

	queryName := kv.getQueryName(args.Cid, args.QueryID)

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "error : not a leader"
		kv.logger(args.Op + fmt.Sprintf(" query(%v) kv server is not a leader", queryName))
		return
	}

	kv.createReplyCh(queryName)
	replyCh := kv.getReplyCh(queryName)

	select {
	case r := <-replyCh:
		if !r.Success {
			reply.Err = "error : " + r.Err
			kv.logger(args.Op + fmt.Sprintf(" query(%v) ", queryName) + string(reply.Err) + " in kv server")
		} else {
			reply.Err = ""
			kv.logger(args.Op + fmt.Sprintf(" query(%v) success, key : %v, new value : %v", queryName, args.Key, r.Value))
		}
	case <-time.After(400 * time.Millisecond):
		kv.logger(args.Op + fmt.Sprintf(" query(%v) time out in kv server", queryName))
		reply.Err = "error : timeout"
	}
	kv.del(queryName)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	select {
	case <-kv.quitCh:
	default:
	}
	kv.quitCh <- struct{}{}
}

func (kv *KVServer) send(ch chan CommonReply, reply CommonReply) {
	select {
	case <-ch:
	default:
	}
	ch <- reply
}

func (kv *KVServer) working() {
	for {
		select {
		case <-kv.quitCh:
			return
		case o := <-kv.applyCh:
			if !o.CommandValid {
				continue
			}
			op := o.Command.(Op)
			queryName := kv.getQueryName(op.Cid, op.QueryID)
			replyCh := kv.getReplyCh(queryName)
			kv.mu.Lock()

			var err Err
			err = ""
			success := true
			ret := ""
			queryID, ok := kv.commandRecord[op.Cid]
			if !ok || queryID < op.QueryID {
				if op.OpType == PUT {
					kv.kvdb[op.Key] = op.Value
				} else if op.OpType == APPEND {
					kv.kvdb[op.Key] += op.Value
				}
				ret = kv.kvdb[op.Key]

				// QUESTION
				if op.OpType != GET {
					kv.commandRecord[op.Cid] = op.QueryID
				}
			} else {
				success = false
				err = "old command"
			}
			kv.mu.Unlock()

			if replyCh != nil {
				r := CommonReply{
					Err:     err,
					Success: success,
					Value:   ret,
				}
				kv.send(replyCh, r)
			}

			// if replyCh != nil {
			// 	r := CommonReply{
			// 		Err:     "debug",
			// 		success: false,
			// 		Value:   "",
			// 	}
			// 	kv.send(replyCh, r)
			// }

		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.quitCh = make(chan struct{}, 1)
	kv.kvdb = make(map[string]string)
	kv.commandRecord = make(map[int64]int)
	kv.replyCh = make(map[string]chan CommonReply)

	go kv.working()

	// You may need initialization code here.

	return kv
}

func (kv *KVServer) logger(content string) {
	log.Printf("kv server(%v) :%v\n", kv.me, content)
}
