package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"fmt"
	"labrpc"
	"math/big"
	"strconv"
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
func (ck *Clerk) queryname(cid int64, queryID int) string {
	return strconv.FormatInt(cid, 10) + "_" + strconv.Itoa(queryID)
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
	cnt := 0
	queryName := ck.queryname(args.Cid, args.QueryID)
	for {
		// try each known server.
		var reply QueryReply
		cnt++
		if cnt > 10 {
			return Config{
				Num: 0,
			}
		}
		fmt.Printf("clerk query config(%v), server : %v, num : %v\n", queryName, lastLeader, num)
		ok := ck.servers[lastLeader].Call("ShardMaster.Query", &args, &reply)
		if ok && reply.WrongLeader == false && reply.Err == "" {
			ck.mu.Lock()
			ck.lastLeader = lastLeader
			ck.mu.Unlock()
			fmt.Printf("clerk query(%v) server : %v, num : %v success\n", queryName, lastLeader, reply.Config.Num)
			return reply.Config
		}
		fmt.Printf("clerk query(%v) server : %v , ok : %v, wrongleader : %v, err : %v\n", queryName, lastLeader, ok, reply.WrongLeader, reply.Err)
		if reply.Err == "old command" {
			return Config{
				Num: 0,
			}
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
	queryName := ck.queryname(args.Cid, args.QueryID)
	cnt := 0
	gids := []int{}
	for key, _ := range servers {
		gids = append(gids, key)
	}
	for {
		// try each known server.
		var reply JoinReply
		cnt++
		fmt.Printf("clerk join config(%v), server : %v, gid : %v\n", queryName, lastLeader, gids)
		ok := ck.servers[lastLeader].Call("ShardMaster.Join", &args, &reply)
		if ok && reply.WrongLeader == false && reply.Err == "" {
			fmt.Printf("clerk join(%v) server : %v, success\n", queryName, lastLeader)
			return
		}
		fmt.Printf("clerk join config(%v), server : %v, ok : %v, wrongleader : %v, err : %v\n", queryName, lastLeader, ok, reply.WrongLeader, reply.Err)
		if reply.Err == "old command" {
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
	queryName := ck.queryname(args.Cid, args.QueryID)
	for {
		// try each known server.
		var reply LeaveReply
		fmt.Printf("clerk leave config(%v), server : %v, gids : %v\n", queryName, lastLeader, gids)
		ok := ck.servers[lastLeader].Call("ShardMaster.Leave", &args, &reply)
		if ok && reply.WrongLeader == false && reply.Err == "" {
			fmt.Printf("clerk leave(%v) server : %v, success\n", queryName, lastLeader)
			return
		}
		fmt.Printf("clerk leave config(%v), server : %v, ok : %v, wrongleader : %v, err : %v\n", queryName, lastLeader, ok, reply.WrongLeader, reply.Err)
		if reply.Err == "old command" {
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
	queryName := ck.queryname(args.Cid, args.QueryID)
	for {
		// try each known server.
		var reply MoveReply

		fmt.Printf("clerk move config(%v), server : %v, move %v shard to %v group\n", queryName, lastLeader, shard, gid)
		ok := ck.servers[lastLeader].Call("ShardMaster.Move", &args, &reply)
		if ok && reply.WrongLeader == false && reply.Err == "" {
			fmt.Printf("clerk move(%v) server : %v, success\n", queryName, lastLeader)
			return
		}
		fmt.Printf("clerk move config(%v), server : %v, ok : %v, wrongleader : %v, err : %v\n", queryName, lastLeader, ok, reply.WrongLeader, reply.Err)
		if reply.Err == "old command" {
			return
		}
		lastLeader = (lastLeader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
