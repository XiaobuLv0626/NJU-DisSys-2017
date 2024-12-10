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
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

const (
	electionMaxTimeout = 300
	electionMinTimeout = 150
	heartbeatTimeout   = 50
)

func randTimeout() time.Duration {
	return time.Duration(electionMinTimeout+rand.Intn(electionMaxTimeout-electionMinTimeout)) * time.Millisecond
}

// log entry contains command for state machine, and term when entry
// was received by leader
type LogEntry struct {
	Command interface{}
	Term    int
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	// State
	state int // 0: follower, 1: candidate, 2: leader

	// Channels
	applyCh chan ApplyMsg // channel to send ApplyMsg to service

	// Timer
	timer *time.Timer // timer for election timeout

	// Voter Count
	votesCount int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == 2)
	return term, isleader

}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	if data == nil || len(data) < 1 {
		// bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)

}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	// receiver: rf; candidate: args, server reply: reply
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	voted_granted := true
	// grant vote if candidate's log is at least as up-to-date as receiver's log and votedFor is null or candidateId
	if len(rf.log) > 0 {
		if args.LastLogTerm < rf.log[len(rf.log)-1].Term {
			voted_granted = false
		} else if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1 {
			voted_granted = false
		}
	}

	if args.Term == rf.currentTerm {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = voted_granted
		return
	}
	// if candidate's term > currentTerm, then receiver is naturally follower. Update currentTerm and grant vote
	if args.Term > rf.currentTerm {
		rf.state = 0 // follower
		rf.currentTerm = args.Term
		if voted_granted {
			rf.votedFor = args.CandidateId
			rf.persist()
		} else {
			rf.votedFor = -1
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = voted_granted
		return
	}

}

// example AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

// example AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if len(rf.log) < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) resendAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// set AppendEntries
		var args AppendEntriesArgs
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex
		args.PrevLogIndex = rf.nextIndex[i] - 1
		args.LeaderCommit = rf.nextIndex[i] - 1

		go func(server int, args AppendEntriesArgs) {
			var reply AppendEntriesReply
			ok := rf.sendAppendEntry(server, args, &reply)
			if ok {
				rf.handleAppendEntries(server, reply)
			}
		}(i, args)
	}
}

func (rf *Raft) resendRequestVote() bool {
	// reset raft peer status
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votesCount = 1
	rf.state = 1 // candidate
	rf.persist()
	// resend RequestVote
	retVal := false
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
	}
	if len(rf.log) > 0 {
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
	}
	for server := range rf.peers {
		if server != rf.me {
			go func(server int, args RequestVoteArgs) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(server, args, &reply)
				if ok {
					rf.handleVoteResult(reply)
				}
			}(server, args)
			retVal = true
		}
	}
	return retVal
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader

}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// handler for follower/leader/candidate when their timer ran out
func (rf *Raft) onTimeout() {
	// Define the action to be performed when the timer reaches 0
	switch rf.state {
	case 0, 1: // follower and candidate: become candidate and start election
		rf.resendRequestVote()
	case 2: // leader: send AppendEntries to all
		rf.resendAppendEntries()
	}
}

// when a timer ran out, set a new one
func (rf *Raft) resetTimer() {
	// for follower and candidate, timer should be a random value between 150ms and 300ms
	timeout := randTimeout()
	if rf.state == 2 {
		// for leader, timer should be heartbeatTimeout
		timeout = heartbeatTimeout * time.Millisecond
	}
	if rf.timer == nil {
		rf.timer = time.NewTimer(timeout)
	} else {
		rf.timer.Reset(timeout)
	}
}

// the defined goroutine of each raft server
func (rf *Raft) runTimer() {
	for {
		<-rf.timer.C
		// Perform the action when the timer reaches 0
		rf.onTimeout()
		// Reset the timer
		rf.resetTimer()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votesCount = 0
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.state = 0 // start as follower

	rf.log = make([]LogEntry, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh // channel to send ApplyMsg to service

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()

	// initialize timer
	rf.resetTimer()
	go rf.runTimer()

	return rf

}
