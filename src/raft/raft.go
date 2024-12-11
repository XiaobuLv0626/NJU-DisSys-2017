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
	timer := electionMinTimeout + rand.Intn(electionMaxTimeout-electionMinTimeout)
	// fmt.Printf("\ntimer:" + strconv.Itoa(timer))
	return time.Duration(timer) * time.Millisecond
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
	reply.VoteGranted = true
	// grant vote if candidate's log is at least as up-to-date as receiver's log and votedFor is null or candidateId
	if len(rf.log) > 0 {
		if args.LastLogTerm < rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1) {
			reply.VoteGranted = false
		}
	}

	if args.Term == rf.currentTerm {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		reply.Term = rf.currentTerm
		return
	}
	// if candidate's term > currentTerm, then receiver is naturally follower. Update currentTerm and grant vote
	if args.Term > rf.currentTerm {
		rf.state = 0 // follower
		rf.currentTerm = args.Term
		if reply.VoteGranted {
			rf.votedFor = args.CandidateId
			rf.persist()
		} else {
			rf.votedFor = -1
		}
		rf.resetTimer()
		reply.Term = rf.currentTerm
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
	Term        int  // currentTerm, for leader to update itself
	Success     bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	CommitIndex int  //
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer rf.resetTimer()

	reply.Success = false
	reply.Term = rf.currentTerm
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// if receives an arg whose term is greater than server's, then
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.state = 1
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	reply.Term = args.Term
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex >= 0 && (args.PrevLogIndex > len(rf.log)-1 || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term) {
		index := min(len(rf.log)-1, args.PrevLogIndex)
		for index >= 0 {
			if args.PrevLogTerm == rf.log[index].Term {
				break
			}
			index -= 1
		}
		reply.CommitIndex = index
		return
	}
	reply.Success = true
	if args.Entries == nil {
		if len(rf.log) > args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			go rf.commitEntries()
		}
		reply.CommitIndex = args.PrevLogIndex
	} else {
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
		if len(rf.log) > args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			go rf.commitEntries()
		}
		reply.CommitIndex = len(rf.log) - 1
	}
}

func (rf *Raft) commitEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.lastApplied + 1
	for index <= rf.commitIndex {
		args := ApplyMsg{
			Index:   index + 1,
			Command: rf.log[index].Command,
		}
		rf.applyCh <- args
		index += 1
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) handleAppendEntries(server int, reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if server is not leader, return.
	if rf.state != 2 {
		return
	}
	// if reply > server.currentTerm, downgrade reply to follower
	if reply.Term > rf.currentTerm {
		rf.state = 0
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.resetTimer()
		return
	}
	// If AppendEntries fails because of login consistency: decrement nextIndex and retry
	if reply.Success == false {
		rf.nextIndex[server] = reply.CommitIndex + 1
		rf.resendAppendEntries()
	} else {
		//If successful: update nextIndex and matchIndex for follower
		rf.nextIndex[server] = reply.CommitIndex + 1
		rf.matchIndex[server] = reply.CommitIndex
		matchCount := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= rf.matchIndex[server] {
				matchCount++
			}
		}
		if matchCount < len(rf.peers)/2+1 {
			return
		}
		if rf.commitIndex < rf.matchIndex[server] && rf.currentTerm == rf.log[rf.matchIndex[server]].Term {
			rf.commitIndex = rf.matchIndex[server]
			go rf.commitEntries()
		}
	}
}

func (rf *Raft) handleVoteResult(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// for reply term older than currentTerm, just ignore
	if reply.Term < rf.currentTerm {
		return
	}
	// for reply term newer than currentTerm, became follower
	if reply.Term > rf.currentTerm {
		rf.state = 0 // follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.resetTimer()
		return
	}
	// if reply term == currentTerm and this vote is for server
	if reply.Term == rf.currentTerm {
		if rf.state == 1 && reply.VoteGranted {
			rf.votesCount += 1
			// if server get majority vote, become a leader
			if rf.votesCount >= len(rf.peers)/2+1 {
				rf.state = 2 // Leader
				for i := range rf.peers {
					if i == rf.me {
						continue
					}
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = -1
				}
				rf.resetTimer()
			}
		}
		return
	}
}

func (rf *Raft) resendAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// set AppendEntries
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			LeaderCommit: rf.commitIndex,
		}
		if args.PrevLogIndex >= 0 {
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		}
		if rf.nextIndex[i] < len(rf.log) {
			args.Entries = rf.log[rf.nextIndex[i]:]
		}
		go func(server int, args AppendEntriesArgs) {
			var reply AppendEntriesReply
			ok := rf.sendAppendEntry(server, args, &reply)
			if ok {
				rf.handleAppendEntries(server, reply)
			}
		}(i, args)
	}
}

func (rf *Raft) resendRequestVote() {
	// reset raft peer status
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votesCount = 1
	rf.state = 1 // candidate
	rf.persist()
	// fmt.Printf("\ncandidate:" + strconv.Itoa(rf.me) + " " + strconv.Itoa(rf.currentTerm))
	// resend RequestVote
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
		}
	}
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
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := rf.state == 2
	// if this server is leader, start the agreement and return immediately
	if isLeader {
		newlog := LogEntry{
			Command: command,
			Term:    rf.currentTerm,
		}
		rf.log = append(rf.log, newlog)

		term = rf.currentTerm
		index = len(rf.log)
		rf.persist()
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Define the action to be performed when the timer reaches 0
	switch rf.state {
	case 0, 1: // follower and candidate: become candidate and start election
		rf.resendRequestVote()
	case 2: // leader: send AppendEntries to all
		rf.resendAppendEntries()
	}
	// Reset the timer
	rf.resetTimer()
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
