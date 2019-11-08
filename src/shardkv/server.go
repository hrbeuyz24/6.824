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
	ID      int
	OpType  string
	Key     string
	Value   string
	Cid     int64
	QueryID int
	Num     int
	Shard   int
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
	kvdb      map[int]map[string]string
	dataState map[int]int
	mck       *shardmaster.Clerk
	config    shardmaster.Config

	toSendData map[string]kvData

	persister     *raft.Persister
	commandRecord map[int64]int
	replyCh       map[string]chan CommonReply

	done             chan struct{}
	updateConfigDone chan struct{}
	sendDataDone     chan struct{}
}

func (sm *ShardKV) getQueryName(cid int64, queryID int) string {
	return strconv.FormatInt(cid, 10) + "_" + strconv.Itoa(queryID)
}

func (sm *ShardKV) getReplyCh(queryName string, flag bool) chan CommonReply {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	ret, ok := sm.replyCh[queryName]
	if !ok && flag {
		sm.replyCh[queryName] = make(chan CommonReply)
		ret = sm.replyCh[queryName]
	}
	return ret
}

func (sm *ShardKV) del(queryName string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	ch, ok := sm.replyCh[queryName]
	if ok {
		delete(sm.replyCh, queryName)
		close(ch)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//kv.logger("try lock 9")
	kv.mu.Lock()
	//kv.logger("lock 9")
	num := kv.config.Num
	kv.mu.Unlock()
	//kv.logger("unlock 9")
	op := Op{
		OpType:  "Get",
		Key:     args.Key,
		Cid:     args.Cid,
		QueryID: args.QueryID,
		Num:     num,
		Shard:   key2shard(args.Key),
	}

	reply.WrongLeader = false
	reply.Err = ""
	reply.Value = ""

	queryName := kv.getQueryName(args.Cid, args.QueryID)
	replyCh := kv.getReplyCh(queryName, true)
	defer kv.del(queryName)

	_, _, isLeader := kv.rf.Start(op)
	kv.logger(fmt.Sprintf("start a get op with key : %v, queryName : %v", args.Key, queryName))
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "error : not a leader"
		kv.logger(fmt.Sprintf("get query(%v) kv server is not a leader", queryName))
		return
	}

	select {
	case r := <-replyCh:
		if !r.Success {
			reply.Err = r.Err
			kv.logger(fmt.Sprintf("get query(%v) ", queryName) + string(reply.Err) + " in kv server")
		} else {
			reply.Err = OK
			reply.Value = r.Value
			kv.logger(fmt.Sprintf("get query(%v) success, key : %v, value : %v", queryName, args.Key, reply.Value))
		}
	case <-time.After(800 * time.Millisecond):
		kv.logger(fmt.Sprintf("get query(%v) time out in kv server", queryName))
		reply.Err = "error : timeout"
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//kv.logger("try lock 8")
	kv.mu.Lock()
	//kv.logger("lock 8")
	num := kv.config.Num
	kv.mu.Unlock()
	//kv.logger("unlock 8")
	op := Op{
		ID:      args.ID,
		Key:     args.Key,
		Value:   args.Value,
		Cid:     args.Cid,
		QueryID: args.QueryID,
		Num:     num,
		Shard:   key2shard(args.Key),
	}
	if args.Op == "Put" {
		op.OpType = "Put"
	} else {
		op.OpType = "Append"
	}

	reply.WrongLeader = false
	reply.Err = ""

	queryName := kv.getQueryName(args.Cid, args.QueryID)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "error : not a leader"
		kv.logger(args.Op + fmt.Sprintf(" query(%v) kv server is not a leader", queryName))
		return
	}
	replyCh := kv.getReplyCh(queryName, true)
	defer kv.del(queryName)
	kv.rf.Start(op)

	kv.logger(fmt.Sprintf("receives PutAppend(%v) ID : %v ,Request op : %v, key : %v, value : %v", queryName, args.ID, args.Op, args.Key, args.Value))

	select {
	case r := <-replyCh:
		if !r.Success {
			reply.Err = r.Err
			kv.logger(args.Op + fmt.Sprintf(" query(%v) ", queryName) + string(reply.Err) + " in kv server")
		} else {
			reply.Err = OK
			kv.logger(args.Op + fmt.Sprintf(" query(%v) success, key : %v, new value : %v", queryName, args.Key, r.Value))
		}
	case <-time.After(800 * time.Millisecond):
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
	kv.sendDataDone <- struct{}{}
	kv.updateConfigDone <- struct{}{}
	kv.done <- struct{}{}
}

func (kv *ShardKV) doSnapshot(applyIndex int) {
	kv.mu.Lock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.commandRecord)
	e.Encode(kv.kvdb)
	e.Encode(kv.toSendData)
	e.Encode(kv.dataState)
	e.Encode(kv.config)
	kv.logger(fmt.Sprintf("snapshot applyindex : %v", applyIndex))
	go kv.rf.SnapShot(applyIndex, w.Bytes())
	kv.mu.Unlock()
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	//kv.logger("try lock 10")
	kv.mu.Lock()
	//kv.logger("lock 10")
	kv.logger("start readSnapshot")
	w := bytes.NewBuffer(data)
	d := labgob.NewDecoder(w)
	if d.Decode(&kv.commandRecord) != nil ||
		d.Decode(&kv.kvdb) != nil ||
		d.Decode(&kv.toSendData) != nil ||
		d.Decode(&kv.dataState) != nil ||
		d.Decode(&kv.config) != nil {
		panic("read Snapshot error")
	}
	kv.mu.Unlock()
	//kv.logger("unlock 10")
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
	labgob.Register(shardmaster.Config{})
	labgob.Register(RequestDBInfoArgs{})
	labgob.Register(RequestDBInfoReply{})
	labgob.Register(UpdateNum{})

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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.done = make(chan struct{})
	kv.updateConfigDone = make(chan struct{})
	kv.sendDataDone = make(chan struct{})

	kv.toSendData = make(map[string]kvData)
	kv.dataState = make(map[int]int)

	kv.kvdb = make(map[int]map[string]string)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.kvdb[i] = make(map[string]string)
	}

	kv.commandRecord = make(map[int64]int)
	kv.replyCh = make(map[string]chan CommonReply)
	kv.persister = persister

	kv.readSnapshot(kv.persister.ReadSnapshot())

	go kv.working()
	go kv.update()
	go kv.sendData()

	return kv
}

func (kv *ShardKV) receiveData(data *RequestDBInfoArgs) {

	queryName := strconv.Itoa(data.Num) + "_" + strconv.Itoa(data.Shard)

	kv.logger(fmt.Sprintf("receive data(%v), shard : %v, num : %v, data : %v", queryName, data.Shard, data.Num, data.Data))
	var err Err = OK
	success := true
	//kv.logger("try lock 7")
	kv.mu.Lock()
	//kv.logger("lock 7")
	if data.Num <= kv.dataState[data.Shard] {
		success = false
		err = ErrOldCommand
		kv.logger(fmt.Sprintf("receive data(%v) too old, shard : %v, num : %v", queryName, data.Shard, data.Num))
	} else {
		for key, value := range data.Data {
			kv.kvdb[data.Shard][key] = value
		}
		for cid, queryId := range data.Rc {
			if _, ok := kv.commandRecord[cid]; !ok || queryId > kv.commandRecord[cid] {
				kv.commandRecord[cid] = queryId
			}
		}
		kv.logger(fmt.Sprintf("receive data(%v), update kv.dataState[%v] from %v to %v", queryName, data.Shard, kv.dataState[data.Shard], data.Num+1))
		kv.dataState[data.Shard] = data.Num + 1
	}
	//kv.logger(fmt.Sprintf("receive date(%v), map : %v", queryName, kv.kvdb))
	replyCh, ok := kv.replyCh[queryName]
	if ok && replyCh != nil {
		select {
		case replyCh <- CommonReply{
			Err:     err,
			Success: success,
		}:
		case <-time.After(800 * time.Millisecond):
		}
		// replyCh <- CommonReply{
		// 	Err:     err,
		// 	Success: success,
		// }
	}
	kv.mu.Unlock()
	//kv.logger("unlock 7")
}

func (kv *ShardKV) UpdateDB(args *RequestDBInfoArgs, reply *RequestDBInfoReply) {
	kv.logger(fmt.Sprintf("receive updateDB rpc, data.Num : %v, data.Shard : %v, data : %v", args.Num, args.Shard, args.Data))
	queryName := strconv.Itoa(args.Num) + "_" + strconv.Itoa(args.Shard)
	reply.Err = OK
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrNotLeader
		return
	}
	replyCh := kv.getReplyCh(queryName, true)
	defer kv.del(queryName)
	kv.rf.Start(*args)
	kv.logger("start commid : " + queryName)
	select {
	case r := <-replyCh:
		if !r.Success {
			reply.Err = r.Err
			kv.logger(fmt.Sprintf("update db(%v) return %v", queryName, reply.Err))
		} else {
			reply.Err = OK
			kv.logger(fmt.Sprintf("update db(%v) success", queryName))
		}
	case <-time.After(800 * time.Millisecond):
		kv.logger(fmt.Sprintf("update db(%v) time out in kv server", queryName))
		reply.Err = "ErrTimeOut"
	}
}

func (kv *ShardKV) sendToGid(k string, data kvData) {
	kv.logger(fmt.Sprintf("to gid : %v, num : %v, shard : %v", data.Gid, data.Num, data.Shard))
	//kv.logger("try lock 5")
	kv.mu.Lock()
	//kv.logger("lock 5")
	if kv.dataState[data.Shard] < data.Num {
		kv.logger(fmt.Sprintf("send data to %v group, data.Num : %v, data.Shard : %v, need to wait data, current shard num : %v", data.Gid, data.Num, data.Shard, kv.dataState[data.Shard]))
		kv.mu.Unlock()
		//kv.logger("unlock 5")
		return
	}

	if data.Gid == kv.gid {
		kv.logger(fmt.Sprintf("update its dataState[%v] from %v to %v", data.Shard, kv.dataState[data.Shard], data.Num+1))
		// kv.dataState[data.Shard] = data.Num + 1
		// if _, ok := kv.toSendData[k]; ok {
		// 	delete(kv.toSendData, k)
		// }
		kv.mu.Unlock()
		kv.rf.Start(UpdateNum{
			Shard: data.Shard,
			Num:   data.Num + 1,
		})
		//kv.logger("unlock 5")
		return
	}
	mp := newMap(kv.kvdb[data.Shard])
	rc := newMapInt(kv.commandRecord)

	kv.mu.Unlock()
	//kv.logger("unlock 5")
	args := RequestDBInfoArgs{
		Num:   data.Num,
		Shard: data.Shard,
		Data:  mp,
		Rc:    rc,
	}
	//kv.logger(fmt.Sprintf("send data to %v group, data.Num : %v, data.Shard : %v, data : %v", data.Gid, data.Num, data.Shard, mp))
	for si := 0; si < len(data.Servers); si++ {
		srv := kv.make_end(data.Servers[si])
		var reply RequestDBInfoReply
		ok := srv.Call("ShardKV.UpdateDB", &args, &reply)
		if !ok {
			kv.logger(fmt.Sprintf("send data to %v group, data.Num : %v, data.Shard : %v, return %v", data.Gid, data.Num, data.Shard, "rpc fail"))
		}
		if ok && (reply.Err == OK || reply.Err == ErrOldCommand) {
			kv.logger(fmt.Sprintf("send data to %v group, data.Num : %v, data.Shard : %v, return %v", data.Gid, data.Num, data.Shard, reply.Err))
			//kv.logger("try lock 6")
			kv.mu.Lock()
			//kv.logger("lock 6")
			if data.Num >= kv.dataState[data.Shard] {
				for key, _ := range mp {
					_, ok := kv.kvdb[data.Shard][key]
					if ok {
						delete(kv.kvdb[data.Shard], key)
					}
				}
			}

			// TODO:
			//kv.dataState[data.Shard] = 0
			_, ok = kv.toSendData[k]
			if ok {
				kv.logger(fmt.Sprintf("delete from toSendData, key : %v", k))
				delete(kv.toSendData, k)
			}
			kv.mu.Unlock()
			//kv.logger("unlock 6")
			return
		}
	}
}

func (kv *ShardKV) sendData() {
	for {
		select {
		case <-kv.sendDataDone:
			return
		default:
			var wg sync.WaitGroup
			//kv.logger("try lock 4")
			kv.mu.Lock()
			//kv.logger("lock 4")
			kv.logger(fmt.Sprintf("start new send data circle, account : %v", len(kv.toSendData)))
			for key, data := range kv.toSendData {
				wg.Add(1)
				go func(k string, d kvData) {
					kv.sendToGid(k, d)
					wg.Done()
				}(key, data)
			}
			kv.mu.Unlock()
			//kv.logger("unlock 4")
			wg.Wait()
			kv.logger("finish one send data circle")
		}
		time.Sleep(time.Millisecond * 150)
	}
}

func (kv *ShardKV) dealCommand(o raft.ApplyMsg) {
	op := o.Command.(Op)
	queryName := kv.getQueryName(op.Cid, op.QueryID)
	kv.logger(fmt.Sprintf("try to commit query(%v), commidindex : %v", queryName, o.CommandIndex))
	//kv.logger("try lock 3")
	kv.mu.Lock()
	//kv.logger("lock 3")
	var err Err
	err = ""
	success := true
	ret := ""
	queryID, ok := kv.commandRecord[op.Cid]

	if op.OpType == "Put" {
		kv.logger(fmt.Sprintf("try to commit Put(%v), key : %v, value : %v", queryName, op.Key, op.Value))
	}

	if !ok || queryID < op.QueryID {
		if op.Num != kv.config.Num || kv.config.Shards[key2shard(op.Key)] != kv.gid {
			success = false
			kv.logger(fmt.Sprintf("commit query(%v) fail, wrong num, op.QueryID : %v, kv.queryID : %v, op.Num : %v, kv.config.Num : %v", queryName, op.QueryID, queryID, op.Num, kv.config.Num))
			err = ErrWrongNum
		} else {

			if op.Num > kv.dataState[op.Shard] {
				success = false
				kv.logger(fmt.Sprintf("commit query(%v) fail, need to wait data, op.QueryID : %v, kv.queryID : %v, op.Num : %v, kv.dataState[%v] : %v, ID : %v", queryName, op.QueryID, queryID, op.Num, op.Shard, kv.dataState[op.Shard], op.ID))
				err = ErrWaitData
			} else {
				shard := key2shard(op.Key)
				if op.OpType == "Put" {
					kv.logger(fmt.Sprintf("commit put query(%v), key : %v, shard : %v, put value : %v", queryName, op.Key, shard, op.Value))
					kv.kvdb[shard][op.Key] = op.Value
				} else if op.OpType == "Append" {
					kv.kvdb[shard][op.Key] += op.Value
					kv.logger(fmt.Sprintf("commit append query(%v), key : %v, shard : %v, ID : %v, new value : %v", queryName, op.Key, shard, op.ID, kv.kvdb[shard][op.Key]))
				}
				ret, _ = kv.kvdb[shard][op.Key]

				if op.OpType != "Get" {
					kv.commandRecord[op.Cid] = op.QueryID
				}
				_, has := kv.kvdb[shard][op.Key]
				if !has {
					success = false
					err = ErrNoKey
				}
			}

		}

	} else {
		success = false
		kv.logger(fmt.Sprintf("commit query(%v) fail, old command, op.QueryID : %v, kv.queryID : %v", queryName, op.QueryID, queryID))
		err = ErrOldCommand
	}
	kv.logger(fmt.Sprintf("map : %v", kv.kvdb))
	replyCh, ok := kv.replyCh[queryName]
	if ok && replyCh != nil {
		select {
		case replyCh <- CommonReply{
			Err:     err,
			Success: success,
			Value:   ret,
		}:
		case <-time.After(800 * time.Millisecond):
		}
		// replyCh <- CommonReply{
		// 	Err:     err,
		// 	Success: success,
		// 	Value:   ret,
		// }
	}
	kv.mu.Unlock()
	//kv.logger("unlock 3")
}

func newMap(mp map[string]string) map[string]string {
	nMap := make(map[string]string)
	for key, value := range mp {
		nMap[key] = value
	}
	return nMap
}

func newMapInt(mp map[int64]int) map[int64]int {
	nMap := make(map[int64]int)
	for key, value := range mp {
		nMap[key] = value
	}
	return nMap
}

func (kv *ShardKV) updateConfig(config shardmaster.Config) {
	//kv.logger("try lock 2")

	kv.mu.Lock()
	//kv.logger("lock 2")
	if config.Num != kv.config.Num+1 {
		kv.logger(fmt.Sprintf("updateConfig error, old num : %v, new num : %v", kv.config.Num, config.Num))
		kv.mu.Unlock()
		return
	}

	kv.logger(fmt.Sprintf("commit update config log, old num : %v, new num : %v, shard : %v", kv.config.Num, config.Num, config.Shards))
	tmp := kv.config.Num
	kv.config.Num = config.Num
	for shard, group := range kv.config.Shards {
		if group == kv.gid {
			dataName := strconv.Itoa(tmp) + "_" + strconv.Itoa(shard)
			kv.logger(fmt.Sprintf("try to send data to %v group, shard : %v, Num : %v", config.Shards[shard], shard, tmp))
			kv.toSendData[dataName] = kvData{
				Gid:   config.Shards[shard],
				Num:   tmp,
				Shard: shard,
				//Data:    data,
				Servers: config.Groups[config.Shards[shard]],
			}
		}
		if config.Shards[shard] == kv.gid && config.Num == 1 {
			kv.logger(fmt.Sprintf("new kv.dataState[%v] : 1", shard))
			kv.dataState[shard] = 1
		}
	}

	kv.config = config
	kv.logger(fmt.Sprintf("commit update config log success with new num : %v", kv.config.Num))
	kv.mu.Unlock()
	//kv.logger("unlock 2")
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
			if _, ok := o.Command.(Op); ok {
				kv.dealCommand(o)
			} else if config, ok := o.Command.(shardmaster.Config); ok {
				kv.updateConfig(config)
			} else if requestDBInfoArgs, ok := o.Command.(RequestDBInfoArgs); ok {
				kv.receiveData(&requestDBInfoArgs)
			} else if updateNum, ok := o.Command.(UpdateNum); ok {
				dataName := strconv.Itoa(updateNum.Num-1) + "_" + strconv.Itoa(updateNum.Shard)
				kv.logger(fmt.Sprintf("receives update itself shard : %v, from %v to %v", updateNum.Shard, kv.dataState[updateNum.Shard], updateNum.Num))
				//kv.logger("try to lock 11")
				kv.mu.Lock()
				//kv.logger("lock 11")
				if kv.dataState[updateNum.Shard] < updateNum.Num {
					kv.dataState[updateNum.Shard] = updateNum.Num
				}
				if _, ok := kv.toSendData[dataName]; ok {
					delete(kv.toSendData, dataName)
				}
				kv.mu.Unlock()
				//kv.logger("unlock 11")
			}

			// do snapshot
			if kv.maxraftstate > 0 && kv.maxraftstate < kv.persister.RaftStateSize() {
				kv.logger("start do snapshot")
				kv.doSnapshot(o.CommandIndex)
			}

		}
	}
}

func (kv *ShardKV) update() {
	for {
		select {
		case <-kv.updateConfigDone:
			return
		default:
			//kv.logger("try lock 1")
			kv.mu.Lock()
			//kv.logger("lock 1")
			num := kv.config.Num
			kv.mu.Unlock()
			//kv.logger("unlock 1")
			config := kv.mck.Query(num + 1)
			kv.logger("update func finish query")
			if config.Num > num {
				kv.rf.Start(config)
			}
		}
		time.Sleep(time.Millisecond * 150)
	}
}

func (kv *ShardKV) logger(content string) {
	log.Printf("shardkv gid(%v), server(%v) :%v\n", kv.gid, kv.me, content)
}
