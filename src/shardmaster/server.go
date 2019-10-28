package shardmaster

import (
	"fmt"
	"labgob"
	"labrpc"
	"raft"
	"sort"
	"strconv"
	"sync"
	"time"
)

type ShardMaster struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	persister *raft.Persister

	// Your data here.

	replyCh       map[string]chan CommonReply
	quitCh        chan struct{}
	commandRecord map[int64]int

	configs []Config // indexed by config num
}

type CommonReply struct {
	Success bool
	Err     Err
	Config  *Config
}

type Op struct {
	// Your data here.
	OpType  string
	Cid     int64
	QueryID int
	Servers map[int][]string
	GIDs    []int
	Shard   int
	Gid     int
	Num     int
}

func (sm *ShardMaster) getQueryName(cid int64, queryID int) string {
	return strconv.FormatInt(cid, 10) + "_" + strconv.Itoa(queryID)
}

func (sm *ShardMaster) getReplyCh(queryName string, flag bool) chan CommonReply {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	ret, ok := sm.replyCh[queryName]
	if !ok && flag {
		sm.replyCh[queryName] = make(chan CommonReply)
		ret = sm.replyCh[queryName]
	}
	return ret
}

func (sm *ShardMaster) del(queryName string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	ch, ok := sm.replyCh[queryName]
	if ok {
		delete(sm.replyCh, queryName)
		close(ch)
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	op := Op{
		OpType:  "Join",
		Cid:     args.Cid,
		QueryID: args.QueryID,
		Servers: args.Servers,
	}

	reply.WrongLeader = false
	reply.Err = ""

	queryName := sm.getQueryName(args.Cid, args.QueryID)
	sm.logger(fmt.Sprintf("recieves Join request(%v)", queryName))

	replyCh := sm.getReplyCh(queryName, true)
	defer sm.del(queryName)

	_, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		sm.logger(fmt.Sprintf("Join request(%v), server is not the leader", queryName))
		reply.WrongLeader = true
		reply.Err = "not a leader"
		return
	}

	select {
	case r := <-replyCh:
		if !r.Success {
			reply.Err = "error : " + r.Err
			sm.logger(fmt.Sprintf("Join request(%v) return error : %v", queryName, reply.Err))
			return
		} else {
			sm.logger(fmt.Sprintf("Join request(%v) return success", queryName))
			return
		}
	case <-time.After(400 * time.Millisecond):
		reply.Err = "time out"
		return
	}

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		OpType:  "Leave",
		Cid:     args.Cid,
		QueryID: args.QueryID,
		GIDs:    args.GIDs,
	}

	reply.WrongLeader = false
	reply.Err = ""

	queryName := sm.getQueryName(args.Cid, args.QueryID)
	sm.logger(fmt.Sprintf("recieves Leave request(%v), leave : %v group", queryName, args.GIDs))

	replyCh := sm.getReplyCh(queryName, true)
	defer sm.del(queryName)

	_, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		sm.logger(fmt.Sprintf("Leave request(%v), server is not the leader", queryName))
		reply.WrongLeader = true
		reply.Err = "not a leader"
		return
	}

	select {
	case r := <-replyCh:
		if !r.Success {
			reply.Err = "error : " + r.Err
			sm.logger(fmt.Sprintf("Leave request(%v) return error : %v", queryName, reply.Err))
			return
		} else {
			sm.logger(fmt.Sprintf("Leave request(%v) return success", queryName))
			return
		}
	case <-time.After(400 * time.Millisecond):
		reply.Err = "time out"
		return
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		OpType:  "Move",
		Cid:     args.Cid,
		QueryID: args.QueryID,
		Shard:   args.Shard,
		Gid:     args.GID,
	}

	reply.WrongLeader = false
	reply.Err = ""

	queryName := sm.getQueryName(args.Cid, args.QueryID)
	sm.logger(fmt.Sprintf("recieves Move request(%v), move %v shard to %v group", queryName, args.Shard, args.GID))

	replyCh := sm.getReplyCh(queryName, true)
	defer sm.del(queryName)

	_, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		sm.logger(fmt.Sprintf("Leave request(%v), server is not the leader", queryName))
		reply.WrongLeader = true
		reply.Err = "not a leader"
		return
	}

	select {
	case r := <-replyCh:
		if !r.Success {
			reply.Err = "error : " + r.Err
			sm.logger(fmt.Sprintf("Leave request(%v) return error : %v", queryName, reply.Err))
			return
		} else {
			sm.logger(fmt.Sprintf("Leave request(%v) return success", queryName))
			return
		}
	case <-time.After(400 * time.Millisecond):
		reply.Err = "time out"
		return
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		OpType:  "Query",
		Cid:     args.Cid,
		QueryID: args.QueryID,
		Num:     args.Num,
	}

	reply.WrongLeader = false
	reply.Err = ""

	queryName := sm.getQueryName(args.Cid, args.QueryID)
	sm.logger(fmt.Sprintf("recieves Query request(%v), query %v config", queryName, args.Num))

	replyCh := sm.getReplyCh(queryName, true)
	defer sm.del(queryName)

	_, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		sm.logger(fmt.Sprintf("Query request(%v), server is not the leader", queryName))
		reply.WrongLeader = true
		reply.Err = "not a leader"
		return
	}

	select {
	case r := <-replyCh:
		if !r.Success {
			reply.Err = "error : " + r.Err
			sm.logger(fmt.Sprintf("Query request(%v) return error : %v", queryName, reply.Err))
			return
		} else {
			reply.Config = *(r.Config)
			sm.logger(fmt.Sprintf("Query request(%v) return success", queryName))
			// if args.Num != reply.Config.Num {
			// 	panic("query return wrong config")
			// }
			return
		}
	case <-time.After(400 * time.Millisecond):
		reply.Err = "time out"
		return
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.quitCh <- struct{}{}
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) createNewConfig() int {
	lastConfig := sm.configs[len(sm.configs)-1]
	config := Config{
		Num:    lastConfig.Num,
		Shards: lastConfig.Shards,
		Groups: make(map[int][]string),
	}
	for key, value := range lastConfig.Groups {
		config.Groups[key] = value
	}
	config.Num = len(sm.configs)
	sm.configs = append(sm.configs, config)
	sm.logger(fmt.Sprintf("lastConfig.Num : %v, lastConfig.Shards : %v, lastConfig.Groups : %v", lastConfig.Num, lastConfig.Shards, lastConfig.Groups))
	return len(sm.configs) - 1
}

func (sm *ShardMaster) divide(config *Config) {
	groupNum := len(config.Groups)
	if groupNum == 0 {
		return
	}

	num := NShards / groupNum
	rest := NShards - num*groupNum

	groupIDs := []int{}
	for key := range config.Groups {
		groupIDs = append(groupIDs, key)
	}
	// 避免map随机顺序访问
	sort.Ints(groupIDs)

	gNum := make(map[int]int)
	for i := 0; i < NShards; i++ {
		gNum[config.Shards[i]]++
	}

	v := make(map[int]bool)
	for i := 0; i < NShards; i++ {
		gid := config.Shards[i]
		if _, ok := config.Groups[gid]; ok && (gNum[gid] <= num || gNum[gid] == num+1 && rest > 0) {
			if gNum[gid] == num+1 {
				gNum[gid]--
				v[gid] = true
				rest--
			}
		} else {
			mark := false
			for _, key := range groupIDs {
				if val, ok := gNum[key]; !ok || val < num {
					gNum[key]++
					gNum[gid]--
					config.Shards[i] = key
					mark = true
					break
				}
			}
			if !mark {
				for _, key := range groupIDs {
					if val, _ := gNum[key]; val == num {
						_, ok := v[key]
						if ok {
							continue
						}
						gNum[gid]--
						config.Shards[i] = key
						rest--
						v[key] = true
						if rest < 0 {
							panic("divide error")
						}
						break
					}
				}
			}
		}
	}

	// TODO
	// g := []int{}
	// for key := range config.Groups {
	// 	g = append(g, key)
	// }
	// length := len(g)
	// sort.Ints(g)
	// for index := 0; index < len(config.Shards); index++ {
	// 	config.Shards[index] = g[index%length]
	// }

	sm.logger(fmt.Sprintf("after divide config.Shards : %v", config.Shards))
}

func (sm *ShardMaster) working() {
	for {
		select {
		case <-sm.quitCh:
			sm.logger("sm server quit")
			return
		case o := <-sm.applyCh:
			op := o.Command.(Op)

			success := true
			var err Err = ""
			var config *Config = nil

			queryName := sm.getQueryName(op.Cid, op.QueryID)
			sm.logger(fmt.Sprintf("try to commit query(%v)", queryName))

			queryID, ok := sm.commandRecord[op.Cid]
			if !ok || queryID < op.QueryID {
				sm.commandRecord[op.Cid] = op.QueryID

				if op.OpType == "Join" {
					index := sm.createNewConfig()
					for key, value := range op.Servers {
						// sm.configs[index].Num++
						sm.configs[index].Groups[key] = value
					}
					sm.divide(&(sm.configs[index]))
					sm.logger(fmt.Sprintf("commit Join query(%v) success", queryName))
				} else if op.OpType == "Leave" {
					index := sm.createNewConfig()
					for _, gid := range op.GIDs {
						//sm.configs[index].Num--
						delete(sm.configs[index].Groups, gid)
					}
					sm.divide(&(sm.configs[index]))
					sm.logger(fmt.Sprintf("commit Leave query(%v) success", queryName))
				} else if op.OpType == "Move" {
					index := sm.createNewConfig()
					sm.configs[index].Shards[op.Shard] = op.Gid
					//sm.divide(&(sm.configs[index]))
					sm.logger(fmt.Sprintf("commit Move query(%v) success", queryName))
				} else if op.OpType == "Query" {
					length := len(sm.configs) - 1
					if length < op.Num || op.Num == -1 {
						config = &sm.configs[length]
					} else {
						config = &sm.configs[op.Num]
					}
					sm.logger(fmt.Sprintf("commit Query query(%v) success, query : %v, result : %v, has %v groups", queryName, op.Num, config.Num, len((*config).Groups)))
				}
			} else {
				err = "old command"
				success = false
				sm.logger(fmt.Sprintf("commit query(%v) fail, old comand", queryName))
			}

			// avoid concurrent map read and write
			replyCh := sm.getReplyCh(queryName, false)
			if replyCh != nil {
				replyCh <- CommonReply{
					Success: success,
					Err:     err,
					Config:  config,
				}
			}
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.quitCh = make(chan struct{})
	sm.replyCh = make(map[string]chan CommonReply)
	sm.commandRecord = make(map[int64]int)

	sm.persister = persister

	// Your code here.

	go sm.working()

	return sm
}

func (sm *ShardMaster) logger(content string) {
	fmt.Printf("sm %v server : %v\n", sm.me, content)
}
