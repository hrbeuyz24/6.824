package raftkv

import (
	"crypto/rand"
	"labrpc"
	"log"
	"math/big"
	"sync"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu         sync.Mutex
	clerkID    int64
	queryId    int
	lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkID = nrand()
	ck.queryId = 0
	ck.lastLeader = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	args := GetArgs{
		Cid:     ck.clerkID,
		QueryID: ck.queryId,
		Key:     key,
	}
	ck.queryId++
	lastLeader := ck.lastLeader
	ck.mu.Unlock()

	ret := ""

	log.Printf("client call a get rpc with key : %v, Cid : %v, QueryID : %v", args.Key, args.Cid, args.QueryID)

	for {
		reply := GetReply{}
		ok := ck.servers[lastLeader].Call("KVServer.Get", &args, &reply)
		if ok && reply.WrongLeader == false && reply.Err == "" {
			ret = reply.Value
			break
		} else if reply.Err == "error : old command" {
			break
		}
		lastLeader = (lastLeader + 1) % len(ck.servers)
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
	return ret
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	args := PutAppendArgs{
		Cid:     ck.clerkID,
		QueryID: ck.queryId,
		Op:      op,
		Key:     key,
		Value:   value,
	}
	ck.queryId++
	lastLeader := ck.lastLeader
	ck.mu.Unlock()

	for {
		log.Printf("clerk call %v server the PutAppend rpc\n", lastLeader)
		reply := PutAppendReply{}
		ok := ck.servers[lastLeader].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.WrongLeader == false && reply.Err == "" {
			break
		} else if reply.Err == "error : old command" {
			break
		}
		lastLeader = (lastLeader + 1) % len(ck.servers)
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	log.Printf("cleak try to put key : %v value : %v\n", key, value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	log.Printf("cleak try to append key : %v value : %v\n", key, value)
	ck.PutAppend(key, value, "Append")
}
