package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"fmt"
	"labrpc"
	"log"
	"math/big"
	r "math/rand"
	"shardmaster"
	"strconv"
	"sync"
	"time"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clerkID int64
	queryId int
	mu      sync.Mutex
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clerkID = nrand()
	ck.queryId = 0
	return ck
}

func (sm *Clerk) getQueryName(cid int64, queryID int) string {
	return strconv.FormatInt(cid, 10) + "_" + strconv.Itoa(queryID)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {

	shard := key2shard(key)
	ck.mu.Lock()
	ck.queryId++
	args := GetArgs{
		Cid:     ck.clerkID,
		QueryID: ck.queryId,
		Key:     key,
		Shard:   shard,
	}
	ck.mu.Unlock()

	queryName := ck.getQueryName(ck.clerkID, ck.queryId)
	for {
		gid := ck.config.Shards[shard]
		args.Num = ck.config.Num
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				fmt.Printf("clerk %v request call %v shardkv.Get, serverid : %v, key : %v, shard : %v, toGid : %v, ck.Num : %v, ck.shard : %v\n", queryName, ck.clerkID, si, key, shard, gid, ck.config.Num, ck.config.Shards)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if !ok {
					fmt.Printf("clerk %v request(%v) rpc request fail\n", "Get", queryName)
					//ck.config = ck.sm.Query(-1)
					continue
				}
				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
					fmt.Printf("clerk %v request(%v) return OK\n", "Get", queryName)
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup || reply.Err == ErrWrongNum) {
					fmt.Printf("clerk %v request(%v) return error : %v\n", "Get", queryName, ErrWrongNum)
					break
				}
				if ok && (reply.Err == ErrOldCommand) {
					fmt.Printf("clerk %v request(%v) return error : %v\n", "Get", queryName, ErrOldCommand)
					return ""
				}
				if reply.Err != "" {
					fmt.Printf("clerk %v request(%v), return error : %v\n", "Get", queryName, reply.Err)
				}
				if reply.WrongLeader == false {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		fmt.Printf("clerk %v query latest config\n", ck.clerkID)
		ck.config = ck.sm.Query(-1)
		fmt.Printf("clerk %v update latest config, num : %v, shards : %v\n", ck.clerkID, ck.config.Num, ck.config.Shards)
	}
}

func randN() int {
	r.Seed(time.Now().UnixNano())
	return r.Intn(10000000000)
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	shard := key2shard(key)
	fmt.Printf("%v clerk PutAppend try to lock ck.mu\n", ck.clerkID)
	ck.mu.Lock()
	fmt.Printf("%v clerk PutAppend lock ck.mu\n", ck.clerkID)

	ck.queryId++
	args := PutAppendArgs{
		Cid:     ck.clerkID,
		QueryID: ck.queryId,
		Op:      op,
		Key:     key,
		Value:   value,
		Shard:   shard,
	}
	ck.mu.Unlock()
	fmt.Printf("%v clerk PutAppend unlock ck.mu\n", ck.clerkID)
	queryName := ck.getQueryName(ck.clerkID, ck.queryId)

	for {
		gid := ck.config.Shards[shard]
		args.Num = ck.config.Num
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				args.ID = randN()
				fmt.Printf("clerk %v request call %v shardkv.PutAppend, serverid : %v, key : %v, value : %v, shard : %v, toGid : %v, ck.Num : %v, ck.shards : %v, ID : %v\n", ck.clerkID, queryName, si, key, value, shard, gid, ck.config.Num, ck.config.Shards, args.ID)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if !ok {
					fmt.Printf("clerk %v request(%v), ID : %v rpc request fail\n", op, queryName, args.ID)
					//ck.config = ck.sm.Query(-1)
					continue
				}
				if ok && reply.WrongLeader == false && reply.Err == OK {
					fmt.Printf("clerk %v request(%v), ID : %v return OK\n", op, queryName, args.ID)
					return
				}
				if ok && (reply.Err == ErrWrongGroup || reply.Err == ErrWrongNum) {

					fmt.Printf("clerk %v request(%v), ID : %v return error : %v\n", op, queryName, args.ID, ErrWrongNum)
					break
				}
				if ok && (reply.Err == ErrOldCommand) {

					fmt.Printf("clerk %v request(%v), ID : %v return error : %v\n", op, queryName, args.ID, ErrOldCommand)
					return
				}

				if reply.Err != "" {
					fmt.Printf("clerk %v request(%v), ID : %v return error : %v\n", op, queryName, args.ID, reply.Err)
				}
				if reply.WrongLeader == false {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		fmt.Printf("clerk %v query latest config\n", ck.clerkID)
		ck.config = ck.sm.Query(-1)
		fmt.Printf("clerk %v update latest config, num : %v, shards : %v\n", ck.clerkID, ck.config.Num, ck.config.Shards)
	}
}

func (ck *Clerk) Put(key string, value string) {
	log.Printf("clerk try to put key : %v value : %v\n", key, value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	log.Printf("clerk try to append key : %v value : %v\n", key, value)
	ck.PutAppend(key, value, "Append")
}
