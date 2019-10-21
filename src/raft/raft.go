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
	"bytes"
	"fmt"
	"labrpc"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/6.824/src/labgob"
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
	Term    int
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

	nextIndex         []int
	matchIndex        []int
	lastIncludedIndex int
	lastIncludedTerm  int

	exitNotify      chan struct{}
	heartBeatNotify chan struct{}
	voteNotify      chan struct{}

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == LEADER
	return term, isleader
}

func (rf *Raft) persistByte() []byte {
	currentTerm := rf.currentTerm
	isVoted := rf.isVoted
	log := rf.log
	lastIncludedIndex := rf.lastIncludedIndex
	lastIncludedTerm := rf.lastIncludedTerm

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(currentTerm)
	e.Encode(isVoted)
	e.Encode(lastIncludedIndex)
	e.Encode(lastIncludedTerm)
	e.Encode(log)
	return w.Bytes()
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

	rf.persister.SaveRaftState(rf.persistByte())
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var isVoted map[int]bool
	var log []LogEntry
	var lastIncludedTerm int
	var lastIncludedIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&isVoted) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&log) != nil {
		panic("read persist failed")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.isVoted = isVoted
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		rf.mu.Unlock()
	}
}

func (rf *Raft) getLogLen() int {
	return len(rf.log) + rf.lastIncludedIndex - 1
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) getLog(index int) LogEntry {
	return rf.log[index-rf.lastIncludedIndex]
}

func (rf *Raft) SnapShot(index int, data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		return
	}
	rf.log = rf.log[index-rf.lastIncludedIndex:]
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.getLog(index).Term
	rf.persister.SaveStateAndSnapshot(rf.persistByte(), data)
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
	Term         int
	Success      bool
	NextIndex    int
	ConflictTerm int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	lastIncludedIndex int
	lastIncludedTerm  int
	data              []byte
}

type InstallSnapshotReply struct {
	term int
}

func (rf *Raft) send(ch chan struct{}) {
	select {
	case <-ch:
	default:
	}
	ch <- struct{}{}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	rf.logger(fmt.Sprintf("receives from %v server request vote rpc", args.CandidateId))
	// lastLogTerm := -1
	// lastLogIndex := -1
	// if len(rf.log) != 0 {
	// 	lastLogTerm = rf.log[len(rf.log)-1].Term
	// 	lastLogIndex = len(rf.log) - 1
	// }
	lastLogIndex := rf.getLogLen()
	lastLogTerm := rf.getLastLog().Term

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

	if lastLogTerm > args.LastLogTerm {
		reply.VoteGranted = false
		return
	}

	rf.logger(fmt.Sprintf("receives vote request rf.lastLogTerm : %v, args.lastLogTerm : %v, rf.lastLogIndex : %v, args.lastLogIndex : %v", lastLogTerm, args.LastLogTerm, lastLogIndex, args.LastLogIndex))

	if lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex {
		reply.VoteGranted = false
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.send(rf.voteNotify)
	//rf.voteNotify <- struct{}{}

	rf.logger(fmt.Sprintf("votes for %v server", args.CandidateId))
	rf.isVoted[args.Term] = true
}

func (rf *Raft) HandleHeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger(fmt.Sprintf("receives from %v server handle heartbeat rpc", args.LeaderId))

	if args.Term >= rf.currentTerm {
		rf.role = FOLLOWER
		rf.logger("turn to follower")
	}

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.logger("send heartBeatNotify to itself")
	rf.send(rf.heartBeatNotify)
	//rf.heartBeatNotify <- struct{}{}

	rf.currentTerm = args.Term
	reply.Success = true

	// prevLogTerm := -1
	// if args.PrevLogIndex != -1 && args.PrevLogIndex <= len(rf.log)-1 {
	// 	prevLogTerm = rf.log[args.PrevLogIndex].Term
	// }

	if args.PrevLogIndex > rf.getLogLen() || rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm {

		reply.NextIndex = 0
		if args.PrevLogIndex > rf.getLogLen() {
			rf.logger(fmt.Sprintf("handle heartbeart return false, args.PrevLogIndex : %v is bigger than len(rf.log) : %v", args.PrevLogIndex, rf.getLogLen()))
			reply.NextIndex = rf.getLogLen() + 1
		} else {

			prevLogTerm := rf.getLog(args.PrevLogIndex).Term

			rf.logger(fmt.Sprintf("handle heartbeat return false, args.PrevLogIndex : %v, args.PrevLogTerm : %v, rf term : %v", args.PrevLogIndex, args.PrevLogTerm, prevLogTerm))
			mark := true
			for i := 1; i < len(rf.log); i++ {
				if rf.log[i].Term == prevLogTerm {
					reply.NextIndex = i
					mark = false
					break
				}
			}
			if mark {
				panic("not find term same as prevLogTerm")
			}
		}

		// if args.PrevLogIndex > len(rf.log)-1 {
		// 	reply.NextIndex = len(rf.log)
		// 	reply.ConflictTerm = -1
		// } else {
		// 	reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		// 	reply.NextIndex = 0
		// }

		reply.Success = false
		return
	}

	currentIndex := args.PrevLogIndex + 1

	rf.log = append(rf.log[:currentIndex-rf.lastIncludedIndex], args.Entries[:]...)

	// for _, logEntry := range args.Entries {
	// 	if currentIndex >= len(rf.log) {
	// 		rf.log = append(rf.log, logEntry)
	// 	} else {
	// 		rf.log[currentIndex] = logEntry
	// 	}
	// 	currentIndex++
	// }

	if args.LeaderCommit > rf.commitIndex {
		tmp := rf.commitIndex
		rf.commitIndex = args.LeaderCommit
		if rf.getLogLen() < rf.commitIndex {
			rf.commitIndex = rf.getLogLen()
		}
		rf.logger(fmt.Sprintf("updateCommitIndex from %v to %v", tmp, rf.commitIndex))
		rf.updateAppliedIndex()
	}
	if len(rf.log) != 0 {
		rf.logger(fmt.Sprintf("after append new log with lastLogTerm : %v, lastLogIndex : %v", rf.getLastLog().Term, rf.getLogLen()))
	}
	rf.persist()
}

func (rf *Raft) sendHeartBeatTo(server int) {
	for {

		log := make([]LogEntry, 0)
		// prevLogIndex := -1
		// prevLogTerm := -1
		rf.mu.Lock()
		reply := &HeartBeatReply{}
		reply.Success = false
		reply.Term = rf.currentTerm
		if rf.role != LEADER {
			rf.mu.Unlock()
			return
		}
		//rf.logger(fmt.Sprintf("nextIndex[%v] : %v", server, rf.nextIndex[server]))
		// if rf.nextIndex[server] != 0 {
		// 	//rf.logger(fmt.Sprintf("nextIndex[%v] : %v", server, rf.nextIndex[server]))
		// 	prevLogIndex = rf.nextIndex[server] - 1
		// 	prevLogTerm = rf.log[prevLogIndex].Term
		// }

		prevLogIndex := rf.nextIndex[server] - 1

		prevLogTerm := rf.getLog(prevLogIndex).Term

		log = append(log, rf.log[rf.nextIndex[server]-rf.lastIncludedIndex:]...)

		rf.logger(fmt.Sprintf("send to %v server logs from %v to %v", server, rf.nextIndex[server], rf.getLogLen()))
		rf.mu.Unlock()

		args := &HeartBeatArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      log,
			LeaderCommit: rf.commitIndex,
		}
		ok := rf.sendHeartBeat(server, args, reply)

		if !ok {
			return
		}

		if reply.Term > args.Term {
			rf.logger("heartbeat reply's term is bigger than currentTerm, be follower")
			rf.beFollower(reply.Term)
			return
		}

		if !reply.Success {
			rf.mu.Lock()
			rf.logger(fmt.Sprintf("receives heartbeat reply from %v fail, rf.nextIndex[server] : %v", server, rf.nextIndex[server]))
			rf.nextIndex[server] = reply.NextIndex
			// if reply.ConflictTerm == -1 {
			// 	rf.nextIndex[server] = reply.NextIndex
			// } else {
			// 	for index, logEntry := range rf.log {
			// 		if logEntry.Term > reply.ConflictTerm {
			// 			rf.nextIndex[server] = index
			// 			break
			// 		}
			// 	}
			// }
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			if rf.matchIndex[server] < args.PrevLogIndex+len(log) {
				rf.logger(fmt.Sprintf("update matchIndex[%v] from %v to %v", server, rf.matchIndex[server], args.PrevLogIndex+len(log)))
				rf.matchIndex[server] = args.PrevLogIndex + len(log)
				rf.updateCommitIndex()
				rf.updateAppliedIndex()
			}

			if rf.nextIndex[server] < rf.matchIndex[server]+1 {
				rf.logger(fmt.Sprintf("update nextIndex[%v] from %v to %v", server, rf.nextIndex[server], rf.matchIndex[server]+1))
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			}
			rf.persist()
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	matchIndex := make([]int, len(rf.matchIndex))
	copy(matchIndex, rf.matchIndex)
	matchIndex[rf.me] = rf.getLogLen()
	sort.Sort(sort.Reverse(sort.IntSlice(matchIndex)))
	N := matchIndex[len(matchIndex)/2]
	rf.logger(fmt.Sprintf("try to update commit index from %v to %v", rf.commitIndex, N))
	if N > rf.commitIndex && rf.getLog(N).Term == rf.currentTerm {
		rf.logger(fmt.Sprintf("updateCommitIndex from %v to %v", rf.commitIndex, N))
		rf.commitIndex = N
		rf.updateAppliedIndex()
	}
}

func (rf *Raft) updateAppliedIndex() {
	rf.logger(fmt.Sprintf("updateAppliedIndex from %v to %v", rf.lastApplied, rf.commitIndex))
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.getLog(i).Command,
			CommandIndex: i,
		}
		rf.applyCh <- applyMsg
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) broadcast() {
	rf.logger("broadcast")

	for i := 0; i < rf.serverNum; i++ {
		if i != rf.me {
			go rf.sendHeartBeatTo(i)
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

	rf.logger(fmt.Sprintf("send to %v server heart beat with prevLogTerm : %v, prevLogIndex : %v", server, args.PrevLogTerm, args.PrevLogIndex))
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = (rf.role == LEADER)
	if !isLeader {
		return index, term, isLeader
	}
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{
		Term:    term,
		Command: command,
	})
	index = rf.getLogLen()
	rf.logger(fmt.Sprintf("leader start append a new log, len : %v", rf.getLogLen()))
	rf.broadcast()
	rf.persist()

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
	rf.persist()
	rf.send(rf.exitNotify)
	//rf.exitNotify <- struct{}{}
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
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) beLeader() {
	rf.mu.Lock()
	rf.role = LEADER
	rf.logger("be leader")
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	for i := 0; i < rf.serverNum; i++ {
		rf.nextIndex = append(rf.nextIndex, rf.getLogLen()+1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	rf.mu.Unlock()
}

func (rf *Raft) beCandidate() {
	rf.mu.Lock()
	rf.role = CANDIDATE
	rf.logger("be candidate")
	rf.currentTerm++
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) startElection() {

	rf.logger("start Election")

	rf.beCandidate()

	rf.mu.Lock()
	// term := -1
	// if rf.getLogLen() != 0 {
	// 	term = rf.log[len(rf.log)-1].Term
	// }
	term := rf.getLastLog().Term
	requestVoteArgs := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  term,
		LastLogIndex: rf.getLogLen(),
	}
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	go func() {
		var v int32 = 0

		rf.mu.Lock()
		_, ok := rf.isVoted[currentTerm]
		if !ok {
			v++
			rf.isVoted[currentTerm] = true
		} else {
			rf.logger(fmt.Sprintf("has voted for %v", currentTerm))
		}
		rf.mu.Unlock()

		for i := 0; i < rf.serverNum; i++ {
			if i != rf.me {

				go func(i int) {
					reply := &RequestVoteReply{}
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
							rf.logger(fmt.Sprintf("receive %v server's vote", i))
							rf.logger(fmt.Sprintf("receive %v votes", tmp))
							if tmp > int32(rf.serverNum/2) {
								rf.beLeader()
								rf.send(rf.heartBeatNotify)
								//rf.heartBeatNotify <- struct{}{}
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
		timeOut := rand.Intn(200) + 150

		role := rf.GetRole()
		switch role {
		case FOLLOWER:
			rf.logger("follower produce")
			select {
			case <-rf.voteNotify:
				rf.logger("receives voteNotify notify")
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
			case <-rf.voteNotify:
			case <-time.After(time.Duration(timeOut) * time.Millisecond):
				rf.startElection()
			}
		case LEADER:
			rf.logger("leader produce")
			time.Sleep(100 * time.Millisecond)
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
	rf.commitIndex = 0
	rf.isVoted = make(map[int]bool)
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{-1, nil})

	rf.exitNotify = make(chan struct{}, 1)
	rf.heartBeatNotify = make(chan struct{}, 1)
	rf.voteNotify = make(chan struct{}, 1)

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.applyCh = applyCh

	rf.role = FOLLOWER

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if len(rf.log) > 0 {
		rf.logger(fmt.Sprintf("last log term : %v", rf.log[len(rf.log)-1].Term))
	} else {
		rf.logger("raft read persist")
	}

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
	lastLogIndex := rf.getLogLen()
	lastLogTerm := rf.getLastLog().Term

	log.Printf("Raft %v server role(%v),term(%v),lastApplied(%v),commitIndex(%v),lastLogIndex(%v),lastLogTerm(%v): %v\n", rf.me, role, rf.currentTerm, rf.lastApplied, rf.commitIndex, lastLogIndex, lastLogTerm, content)
}
