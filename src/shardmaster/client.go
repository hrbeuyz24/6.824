package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	lastLeader int
	Cid        int64
	QueryID    int
	mu         sync.Mutex
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
	ck.Cid = nrand()
	ck.QueryID = 0
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{}
	// Your code here.
	args.Num = num
	ck.mu.Lock()
	args.Cid = ck.Cid
	ck.QueryID++
	args.QueryID = ck.QueryID
	lastLeader := ck.lastLeader
	ck.mu.Unlock()
	for {
		// try each known server.
		var reply QueryReply
		ok := ck.servers[lastLeader].Call("ShardMaster.Query", &args, &reply)
		if ok && reply.WrongLeader == false && reply.Err == "" {
			return reply.Config
		}
		lastLeader = (lastLeader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{}
	// Your code here.
	args.Servers = servers

	ck.mu.Lock()
	args.Cid = ck.Cid
	ck.QueryID++
	args.QueryID = ck.QueryID
	lastLeader := ck.lastLeader
	ck.mu.Unlock()
	for {
		// try each known server.
		var reply JoinReply
		ok := ck.servers[lastLeader].Call("ShardMaster.Join", &args, &reply)
		if ok && reply.WrongLeader == false && reply.Err == "" {
			return
		}
		lastLeader = (lastLeader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	ck.mu.Lock()
	args.Cid = ck.Cid
	ck.QueryID++
	args.QueryID = ck.QueryID
	lastLeader := ck.lastLeader
	ck.mu.Unlock()
	for {
		// try each known server.
		var reply LeaveReply
		ok := ck.servers[lastLeader].Call("ShardMaster.Leave", &args, &reply)
		if ok && reply.WrongLeader == false && reply.Err == "" {
			return
		}
		lastLeader = (lastLeader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	ck.mu.Lock()
	args.Cid = ck.Cid
	ck.QueryID++
	args.QueryID = ck.QueryID
	lastLeader := ck.lastLeader
	ck.mu.Unlock()
	for {
		// try each known server.
		var reply MoveReply
		ok := ck.servers[lastLeader].Call("ShardMaster.Move", &args, &reply)
		if ok && reply.WrongLeader == false && reply.Err == "" {
			return
		}
		lastLeader = (lastLeader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
