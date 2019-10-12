package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

const (
	FOLLOWER int = iota
	CANDIDATE
	LEADER
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	isVoted     map[int]bool
	serverNum   int
	commitIndex int
	log         []LogEntry
	lastApplied int
	role        int

	nextIndex  []int
	matchIndex []int

	exitNotify      chan struct{}
	heartBeatNotify chan struct{}
	voteNotify      chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == LEADER
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type HeartBeatArgs struct {
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

type HeartBeatReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	rf.logger(fmt.Sprintf("receives from %v server request vote rpc", args.CandidateId))
	lastLogTerm := -1
	lastLogIndex := -1
	if len(rf.log) != 0 {
		lastLogTerm = rf.log[len(rf.log)-1].term
		lastLogIndex = len(rf.log) - 1
	}
	term := rf.currentTerm
	_, ok := rf.isVoted[args.Term]
	rf.mu.Unlock()

	if args.Term > term {
		rf.logger("receive vote request's term is bigger than currentTerm, be follower")
		rf.beFollower(args.Term)
	}

	reply.VoteGranted = true
	reply.Term = -1

	if term > args.Term {
		reply.Term = term
		reply.VoteGranted = false
		return
	}

	if ok {
		reply.VoteGranted = false
		return
	}

	if lastLogTerm > args.Term {
		reply.VoteGranted = false
		return
	}

	if lastLogTerm == args.Term && lastLogIndex > args.LastLogIndex {
		reply.VoteGranted = false
		return
	}

	rf.mu.Lock()
	rf.voteNotify <- struct{}{}
	rf.logger(fmt.Sprintf("votes for %v server", args.CandidateId))
	rf.isVoted[args.Term] = true
	rf.mu.Unlock()
}

func (rf *Raft) HandleHeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger(fmt.Sprintf("receives from %v server handle heartbeat rpc", args.LeaderId))
	// lastLogTerm := -1
	// lastLogIndex := -1
	// if len(rf.log) != 0 {
	// 	lastLogTerm = rf.log[len(rf.log)-1].term
	// 	lastLogIndex = len(rf.log) - 1
	// }

	if args.Term > rf.currentTerm {
		rf.role = FOLLOWER
	}

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.currentTerm = args.Term
	reply.Success = true
	rf.logger("send heartBeatNotify to itself")
	rf.heartBeatNotify <- struct{}{}

}

//sendHeartBeat

func (rf *Raft) sendHeartBeatTo(server int, args HeartBeatArgs, log []LogEntry) {
	reply := &HeartBeatReply{}
	ok := rf.sendHeartBeat(server, &args, reply)
	if ok {
		if reply.Term > args.Term {
			rf.logger("heartbeat reply's term is bigger than currentTerm, be follower")
			rf.beFollower(reply.Term)
		}
	}
}

func (rf *Raft) broadcast() {
	rf.mu.Lock()
	lastLogIndex := -1
	lastLogTerm := -1
	args := HeartBeatArgs{
		Term:         rf.currentTerm,
		PrevLogIndex: lastLogIndex,
		PrevLogTerm:  lastLogTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	log := make([]LogEntry, len(rf.log))
	copy(log, rf.log)
	rf.logger("broadcast")
	rf.mu.Unlock()

	for i := 0; i < rf.serverNum; i++ {
		if i != rf.me {
			go rf.sendHeartBeatTo(i, args, log)
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.logger(fmt.Sprintf("send to %v server request vote", server))
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat(server int, args *HeartBeatArgs, reply *HeartBeatReply) bool {

	rf.logger(fmt.Sprintf("send to %v server heart beat", server))
	ok := rf.peers[server].Call("Raft.HandleHeartBeat", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) GetRole() int {
	rf.mu.Lock()
	role := rf.role
	rf.mu.Unlock()
	return role
}

func (rf *Raft) GetTerm() int {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()
	return term
}

func (rf *Raft) beFollower(term int) {
	rf.mu.Lock()
	rf.role = FOLLOWER
	rf.currentTerm = term
	rf.logger("be follower")
	rf.mu.Unlock()
}

func (rf *Raft) beLeader() {
	rf.mu.Lock()
	rf.role = LEADER
	rf.logger("be leader")
	rf.nextIndex = make([]int, rf.serverNum)
	rf.matchIndex = make([]int, rf.serverNum)
	rf.mu.Unlock()
}

func (rf *Raft) beCandidate() {
	rf.mu.Lock()
	rf.role = CANDIDATE
	rf.logger("be candidate")
	rf.currentTerm++
	rf.mu.Unlock()
}

func (rf *Raft) startElection() {

	rf.logger("start Election")

	rf.beCandidate()

	rf.mu.Lock()
	term := -1
	if len(rf.log) != 0 {
		term = rf.log[len(rf.log)-1].term
	}
	requestVoteArgs := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  term,
		LastLogIndex: len(rf.log) - 1,
	}
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	reply := &RequestVoteReply{}

	go func() {
		var v int32 = 0
		_, ok := rf.isVoted[currentTerm]
		if !ok {
			v++
		} else {
			rf.logger(fmt.Sprintf("has voted for %v", currentTerm))
		}

		for i := 0; i < rf.serverNum; i++ {
			if i != rf.me {

				go func(i int) {
					ok := rf.sendRequestVote(i, requestVoteArgs, reply)
					role := rf.GetRole()
					term := rf.GetTerm()
					if role != CANDIDATE || currentTerm != term {
						return
					}
					if ok {
						if reply.VoteGranted {
							atomic.AddInt32(&v, 1)
							tmp := atomic.LoadInt32(&v)
							rf.logger(fmt.Sprintf("receive %v votes", tmp))
							if tmp > int32(rf.serverNum/2) {
								rf.beLeader()
								rf.heartBeatNotify <- struct{}{}
								rf.broadcast()
							}
						} else {
							if reply.Term > currentTerm {
								rf.logger("vote request's term is bigger than currentTerm, be follower")
								rf.beFollower(reply.Term)
								return
							}
						}
					}
				}(i)

			}
		}
		// if v > rf.serverNum/2 {
		// 	rf.beLeader()
		// 	rf.heartBeatNotify <- struct{}{}
		// 	rf.broadcast()
		// }
	}()
}

func (rf *Raft) working() {

	for {
		select {
		case <-rf.exitNotify:
			return
		default:
		}

		//r := rand.New(rand.NewSource(time.Now().UnixNano()))
		timeOut := rand.Intn(250) + 150

		role := rf.GetRole()
		switch role {
		case FOLLOWER:
			rf.logger("follower produce")
			select {
			case <-rf.voteNotify:
			case <-rf.heartBeatNotify:
				rf.logger("receives heart beat notify")
			case <-time.After(time.Duration(timeOut) * time.Millisecond):
				rf.startElection()
			}
		case CANDIDATE:
			rf.logger("candidate produce")
			select {
			case <-rf.heartBeatNotify:
				rf.logger("receives heart beat notify")
			case <-time.After(time.Duration(timeOut) * time.Millisecond):

				rf.startElection()
			}
		case LEADER:
			rf.logger("leader produce")
			time.Sleep(20 * time.Millisecond)
			rf.broadcast()
		}
	}

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.serverNum = len(rf.peers)
	rf.commitIndex = -1
	rf.isVoted = make(map[int]bool)
	rf.lastApplied = -1
	rf.log = make([]LogEntry, 0)

	rf.exitNotify = make(chan struct{}, 3)
	rf.heartBeatNotify = make(chan struct{}, 3)
	rf.voteNotify = make(chan struct{}, 3)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.beFollower(-1)

	go rf.working()

	return rf
}

func (rf *Raft) logger(content string) {
	role := ""
	if rf.role == FOLLOWER {
		role = "follower"
	}
	if rf.role == LEADER {
		role = "leader"
	}
	if rf.role == CANDIDATE {
		role = "candidate"
	}
	fmt.Printf("%v server role(%v),term(%v) : %v\n", rf.me, role, rf.currentTerm, content)

}
