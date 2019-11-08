package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrOldCommand = "ErrOldCommand"
	ErrWaitData   = "ErrWaitData"
	ErrWrongNum   = "ErrWrongNum"
	ErrTimeOut    = "ErrTimeOut"
	ErrNotLeader  = "ErrNotLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cid     int64
	QueryID int
	Shard   int
	Num     int
	ID      int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Cid     int64
	QueryID int
	Shard   int
	Num     int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type CommonReply struct {
	Err     Err
	Value   string
	Success bool
}

type RequestDBInfoArgs struct {
	Num   int
	Shard int
	Data  map[string]string
	Rc    map[int64]int
}

type RequestDBInfoReply struct {
	Err Err
}

type kvData struct {
	Gid   int
	Num   int
	Shard int
	//Data    map[string]string
	Servers []string
}

type UpdateNum struct {
	Shard int
	Num   int
}
