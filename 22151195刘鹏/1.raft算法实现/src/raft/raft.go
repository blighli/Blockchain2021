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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	electionTimer  *time.Timer
	heartBeatTimer *time.Timer

	votedFor    int          //candidateId that received vote in current term
	nodeState   int          //节点的状态，Follower Candidate Leader
	nodeVote    int          //节点的选票数量
	currentTerm int          //当前的term
	logEntries  []LogEntries //
	commitIndex int          // initialized to 0
	lastApplied int          // initialized to 0

}

const (
	// 0
	Follower int = 0
	// 1
	Candidate int = 1
	// 2
	Leader int = 2
	//
	Timeout int = 150
)

func (rf *Raft) SetState(nodeState int) {
	switch nodeState {
	case Follower:
		{
			rf.votedFor = -1
			rf.nodeState = Follower
		}
	case Candidate:
		{
			rf.nodeState = Candidate
		}
	case Leader:
		{
			rf.nodeState = Leader
			rf.electionTimer.Stop()
			rf.HeartBeat()
			rf.heartBeatTimer.Reset(heartBeatTimeout())
		}
	}
}

func (rf *Raft) SetCurrentTerm(term int) {
	rf.currentTerm = term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.nodeState == Leader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

//
// a struct to hold information about each log entry.
//
//
type LogEntries struct {
}

//
// log entries args
//
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntries
	LeaderCommit int
}

//
// log entries Results
//
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("node %v: Receive votes request from %v\n", rf.me, args.CandidateId)
	// rule 1
	if args.Term < rf.currentTerm || (rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		DPrintf("{node %v}: before rf term\n", rf.me)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("{node %v}: vote no\n", rf.me)
		return
	}
	if args.Term > rf.currentTerm {
		rf.SetCurrentTerm(args.Term)
		rf.SetState(Follower)
	}
	reply.VoteGranted, reply.Term = true, rf.currentTerm
	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(randomElecTimeOut())
	DPrintf("{node %v}: vote yes\n", rf.me)

}

//
// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//resets the election timeout
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("rfTerm is %v, args term is %v\n", rfTerm, args.Term)
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// if currentTerm is less than args, change to follower
	if rf.currentTerm < args.Term {
		rf.SetState(Follower)
		rf.SetCurrentTerm(args.Term)
		reply.Term = args.Term
	}
	reply.Success = true
	rf.electionTimer.Reset(randomElecTimeOut())
}

//
//
func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		DPrintf("hello ticker")
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			{
				rf.mu.Lock()
				DPrintf("{node %v}: election timeout\n", rf.me)
				//rf.nodeState = Candidate
				rf.SetState(Candidate)
				rf.currentTerm += 1

				rf.StartElection()

				rf.electionTimer.Reset(randomElecTimeOut())
				rf.mu.Unlock()

			}
		case <-rf.heartBeatTimer.C:
			{
				rf.mu.Lock()
				DPrintf("{node %v}: heartbeat timeout\n", rf.me)
				if rf.nodeState == Leader {
					rf.HeartBeat()
					rf.heartBeatTimer.Reset(heartBeatTimeout())
				}
				rf.mu.Unlock()
			}
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
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		nodeVote:       0,
		nodeState:      Follower,
		currentTerm:    0,
		votedFor:       -1,
		electionTimer:  time.NewTimer(randomElecTimeOut()),
		heartBeatTimer: time.NewTimer(heartBeatTimeout()),
	}
	// Your initialization code here (2A, 2B, 2C).
	// 2A TO DO: goroutine that will kick off leader election periodically
	DPrintf("rf : %v\n", rf)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) StartElection() {

	// vote for itself
	rf.votedFor = rf.me
	rf.nodeVote = 1
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	DPrintf("{node %v}: start election, in term %v\n", args.CandidateId, args.Term)
	// send requestRPC to other servers
	for i := range rf.peers {
		if i != rf.me {
			go func(dest int) {
				reply := RequestVoteReply{
					VoteGranted: false,
				}
				if rf.sendRequestVote(dest, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					DPrintf("{Node %v}: receives requestVoteResponse %v from %v in term %v, reply term is %v, nodestate is %v\n", rf.me, reply, dest, rf.currentTerm, reply.Term, rf.nodeState)
					if rf.currentTerm == reply.Term && rf.nodeState == Candidate {
						if reply.VoteGranted {
							rf.nodeVote++
							if rf.nodeVote*2 >= len(rf.peers) {
								DPrintf("{Node %v}: receives majority votes in term %v", rf.me, rf.currentTerm)
								rf.SetState(Leader)
							}
						} else {
							//convert to follower
							if reply.Term > args.Term {
								rf.SetState(Follower)
								rf.currentTerm = reply.Term
							}
							DPrintf("{node %v}: dont receive node %v vote, cause reply term is %v\n", args.CandidateId, dest, reply.Term)
						}
					}
				}
				// ignore false request
			}(i)
		}
	}

}

func randomElecTimeOut() time.Duration {
	return time.Duration(Timeout+rand.Intn(Timeout)) * time.Millisecond
}

func heartBeatTimeout() time.Duration {
	return time.Duration(Timeout) * time.Millisecond
}

func (rf *Raft) HeartBeat() {

	args := AppendEntriesArgs{
		Term: rf.currentTerm,
	}
	for i := range rf.peers {
		if i != rf.me {
			// 是否有优化的地方，比如已经变成follow，不再发送heartbeat
			go func(dest int) {
				reply := AppendEntriesReply{
					Success: false,
				}
				if rf.SendAppendEntries(dest, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.nodeState == Leader {
						if !reply.Success && reply.Term > args.Term {
							rf.SetState(Follower)
							DPrintf("node %v: dest is %v, I change to follower, success is %v, reply term is %v, args term is %v\n", rf.me, dest, reply.Success, reply.Term, args.Term)
						}
					}
				}
			}(i)
		}
	}
}
