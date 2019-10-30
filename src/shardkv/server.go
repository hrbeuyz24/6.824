package shardkv

// import "shardmaster"
import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"shardmaster"
	"strconv"
	"sync"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	OpType  string
	Key     string
	Value   string
	Cid     int64
	QueryID int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	shards           map[int]bool
	currentConfigNum int
	kvdb             map[string]string
	mck              *shardmaster.Clerk

	persister     *raft.Persister
	commandRecord map[int64]int
	replyCh       map[string]chan CommonReply

	done             chan struct{}
	updateConfigDone chan struct{}
}

func (kv *ShardKV) getQueryName(cid int64, queryID int) string {
	return strconv.FormatInt(cid, 10) + "_" + strconv.Itoa(queryID)
}

func (kv *ShardKV) createReplyCh(queryName string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.replyCh[queryName]
	if !ok {
		kv.replyCh[queryName] = make(chan CommonReply, 1)
	}
}

func (kv *ShardKV) getReplyCh(queryName string) chan CommonReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ret := kv.replyCh[queryName]
	return ret
}

func (kv *ShardKV) del(queryName string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, ok := kv.replyCh[queryName]
	if ok {
		ch := kv.replyCh[queryName]
		delete(kv.replyCh, queryName)
		close(ch)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	// TODO
	kv.mu.Lock()
	if _, ok := kv.shards[args.Shard]; !ok {
		reply.Err = ErrWrongGroup
		reply.WrongLeader = false
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		OpType:  "Get",
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
			reply.Err = r.Err
			kv.logger(fmt.Sprintf("get query(%v) ", queryName) + string(reply.Err) + " in kv server")
		} else {
			reply.Err = OK
			kv.logger(fmt.Sprintf("get query(%v) success, key : %v, value : %v", queryName, args.Key, reply.Value))
			reply.Value = r.Value
		}
	case <-time.After(400 * time.Millisecond):
		kv.logger(fmt.Sprintf("get query(%v) time out in kv server", queryName))
		reply.Err = "error : timeout"
	}

	kv.del(queryName)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	if _, ok := kv.shards[args.Shard]; !ok {
		reply.Err = ErrWrongGroup
		reply.WrongLeader = false
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Key:     args.Key,
		Value:   args.Value,
		Cid:     args.Cid,
		QueryID: args.QueryID,
	}
	if args.Op == "Put" {
		op.OpType = "Put"
	} else {
		op.OpType = "Append"
	}

	reply.WrongLeader = false
	reply.Err = ""

	queryName := kv.getQueryName(args.Cid, args.QueryID)

	kv.logger(fmt.Sprintf("receives PutAppend(%v) Request op : %v, key : %v, value : %v", queryName, args.Op, args.Key, args.Value))

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "error : not a leader"
		kv.logger(args.Op + fmt.Sprintf(" query(%v) kv server is not a leader", queryName))
		return
	}

	kv.createReplyCh(queryName)
	replyCh := kv.getReplyCh(queryName)

	defer kv.del(queryName)

	select {
	case r := <-replyCh:
		if !r.Success {
			reply.Err = r.Err
			kv.logger(args.Op + fmt.Sprintf(" query(%v) ", queryName) + string(reply.Err) + " in kv server")
		} else {
			reply.Err = OK
			kv.logger(args.Op + fmt.Sprintf(" query(%v) success, key : %v, new value : %v", queryName, args.Key, r.Value))
		}
	case <-time.After(400 * time.Millisecond):
		kv.logger(args.Op + fmt.Sprintf(" query(%v) time out in kv server", queryName))
		reply.Err = "error : timeout"
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.

	// kill updateConfig goroutine first.
	kv.updateConfigDone <- struct{}{}
	kv.done <- struct{}{}
}

func (kv *ShardKV) doSnapshot(applyIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.commandRecord)
	e.Encode(kv.kvdb)
	e.Encode(kv.currentConfigNum)
	e.Encode(kv.shards)
	go kv.rf.SnapShot(applyIndex, w.Bytes())
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	kv.mu.Lock()
	kv.logger("start readSnapshot")
	w := bytes.NewBuffer(data)
	d := labgob.NewDecoder(w)
	if d.Decode(&kv.commandRecord) != nil ||
		d.Decode(&kv.kvdb) != nil ||
		d.Decode(&kv.currentConfigNum) != nil ||
		d.Decode(&kv.shards) != nil {
		panic("read Snapshot error")
	}
	kv.mu.Unlock()
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.currentConfigNum = 0
	kv.shards = make(map[int]bool)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.done = make(chan struct{})
	kv.updateConfigDone = make(chan struct{})

	kv.kvdb = make(map[string]string)

	kv.commandRecord = make(map[int64]int)
	kv.replyCh = make(map[string]chan CommonReply)
	kv.persister = persister

	kv.readSnapshot(kv.persister.ReadSnapshot())

	go kv.working()
	go kv.updateConfig()

	return kv
}

func (kv *ShardKV) working() {
	for {

		select {
		case <-kv.done:
			return
		case o := <-kv.applyCh:
			if !o.CommandValid {
				kv.readSnapshot(o.SnapShotData)
				continue
			}
			op := o.Command.(Op)
			queryName := kv.getQueryName(op.Cid, op.QueryID)
			kv.logger(fmt.Sprintf("try to commit query(%v)", queryName))
			//kv.mu.Lock()

			var err Err
			err = ""
			success := true
			ret := ""
			queryID, ok := kv.commandRecord[op.Cid]
			if !ok || queryID < op.QueryID {
				if op.OpType == "Put" {
					kv.logger(fmt.Sprintf("commit put query(%v), key : %v, put value : %v", queryName, op.Key, op.Value))
					kv.kvdb[op.Key] = op.Value
				} else if op.OpType == "Append" {
					kv.kvdb[op.Key] += op.Value
					kv.logger(fmt.Sprintf("commit append query(%v), key : %v, new value : %v", queryName, op.Key, kv.kvdb[op.Key]))
				}

				ret = kv.kvdb[op.Key]

				// QUESTION
				if op.OpType != "Get" {
					kv.commandRecord[op.Cid] = op.QueryID
					_, has := kv.kvdb[op.Key]
					if !has {
						success = false
						err = ErrNoKey
					}
				}
			} else {
				success = false
				kv.logger(fmt.Sprintf("commit query(%v) fail, old command, op.QueryID : %v, kv.queryID : %v", queryName, op.QueryID, queryID))
				err = ErrOldCommand
			}

			if kv.maxraftstate > 0 && kv.maxraftstate < kv.persister.RaftStateSize() {
				kv.logger("start do snapshot")
				kv.doSnapshot(o.CommandIndex)
			}
			kv.mu.Lock()
			replyCh, ok := kv.replyCh[queryName]
			if ok && replyCh != nil {
				replyCh <- CommonReply{
					Err:     err,
					Success: success,
					Value:   ret,
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) updateConfig() {
	ticker := time.NewTicker(time.Millisecond * 100)
	for {
		select {
		case <-kv.updateConfigDone:
			ticker.Stop()
			return
		case <-ticker.C:
			config := kv.mck.Query(-1)
			if config.Num > kv.currentConfigNum {
				kv.logger(fmt.Sprintf("update %v config", config.Num))
				kv.mu.Lock()
				kv.currentConfigNum = config.Num
				kv.shards = make(map[int]bool)
				for shard, group := range config.Shards {
					if group == kv.gid {
						kv.shards[shard] = true
					}
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *ShardKV) logger(content string) {
	log.Printf("shardkv server(%v) :%v\n", kv.me, content)
}
