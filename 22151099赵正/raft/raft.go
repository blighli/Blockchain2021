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
	"sync"
	"sync/atomic"
    "time"
    "math/rand"

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

// 2A
const(
	LEADER = iota
	FOLLOWER
	CANDIDATE
)

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

// 2A
type LogEntry struct {
    Command interface{}
    Term    int
    Index   int
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
	
	// 2A
	// Persistent state on all servers
    currentTerm		int		        	// server已知的最新term
	votedFor		int	                // 当前term下所投票的id,如果没投票则为null
	logs			[]LogEntry	        // 日志，记录状态机命令和从leader接收到日志的term

	// Volatile state on all servers
	commitIndex		int		        	// 将被提交的日志索引
	lastApplied		int		        	// 已被提交到状态机的最后一个日志索引

	// Volatile state on leaders
	nextIndex		[]int	            // 对每个server下一个要发送的日志索引
	matchIndex		[]int	            // 对每个server，已知的最高的复制成功索引

	// Self defined
	voteCounts		int		        	// 当前term中获得的票数
	currentState	int		        	// 当前server状态
	leaderId		int		        	// follower的leaderId
	timer			*time.Timer	        // 定时器
	electionTimeout	time.Duration		// 100-500ms
	applyCh			chan ApplyMsg		// what to do?
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	// 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.currentState == LEADER)
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
	// 2A
	Term			int		// 候选人任期号
	CandidateId		int		// 请求投票的候选人Id
	LastLogIndex	int		// 候选人最新日志的索引
	LastLogTerm		int		// 候选人最新日志的任期
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// 2A
	Term			int		// 当前任期
	VoteGranted		bool	// 候选人是否获得了选票
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()

	flag := true

    // 当前节点日志比请求者新
	if len(rf.logs) > 0 {
		if rf.logs[len(rf.logs)-1].Term > args.LastLogTerm || 
		  (rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && len(rf.logs)-1 > args.LastLogIndex) { flag = false }
	}

    // 当前节点Term大于请求者，忽略请求
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return 
	}

    // 当前节点Term小于请求者
    if rf.currentTerm < args.Term {
       	rf.currentState = FOLLOWER
       	rf.currentTerm = args.Term
       	rf.votedFor = -1

		if flag { rf.votedFor = args.CandidateId }

        rf.resetTimer()
        reply.Term = args.Term
        reply.VoteGranted = (rf.votedFor == args.CandidateId)
        return
    }

    // Term相同，判断是否需要投票
    if rf.currentTerm == args.LastLogTerm {
        if rf.votedFor == -1 && flag { rf.votedFor = args.CandidateId }

        //rf.resetTimer()
        reply.Term = rf.currentTerm
        reply.VoteGranted = (rf.votedFor == args.CandidateId)
    	return 
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

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

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

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)
	
	rf.commitIndex = -1
	rf.lastApplied = -1
	
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.currentState = FOLLOWER
	rf.applyCh = applyCh

	rf.voteCounts = 0
	rf.electionTimeout = time.Millisecond * time.Duration(100 + rand.Intn(400))
	rf.resetTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

// 2A
func (rf *Raft) resetTimer() {
    if rf.timer != nil { rf.timer.Stop() }
    rf.timer = time.AfterFunc(rf.electionTimeout, func(){ rf.TimeoutElection() })
}

// 节点超时后开始选举
func (rf *Raft) TimeoutElection(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.currentState != LEADER {
		rf.currentState = CANDIDATE
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.voteCounts = 1

		args := RequestVoteArgs{
			Term:		    rf.currentTerm,		// 请求者的任期
			CandidateId:	rf.me,			    // 请求者ID
			LastLogIndex:	len(rf.logs) - 1,	// 请求者的最后日志条目的索引值
		}
		
		if len(rf.logs) > 0 { args.LastLogTerm = rf.logs[len(rf.logs)-1].Term }
		
		for i := 0; i < len(rf.peers); i ++ {
			if i == rf.me { continue }
			go func(i int, args RequestVoteArgs) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(i, &args, &reply)
				if ok { rf.handleVoteResult(reply) }
			}(i, args)
		}
	}
	rf.resetTimer()
}

// 2A
// 用于投票结果reply返回后进行处理
func (rf *Raft) handleVoteResult(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reply的任期小于当前任期：投票无效
	if reply.Term < rf.currentTerm { return }
	
	// reply的任期大于当前任期，当前用户成为FOLLOWER，更新任期
	if reply.Term > rf.currentTerm {
		rf.currentState = FOLLOWER
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.resetTimer()
		return 
	}

	// 投票有效
	if rf.currentState == CANDIDATE && reply.VoteGranted {
		rf.voteCounts += 1
		if rf.voteCounts > len(rf.peers)/2 {
			rf.currentState = LEADER
			for i := 0; i < len(rf.peers); i ++ {
				if i == rf.me { continue }
				rf.nextIndex[i] = len(rf.logs)
				rf.matchIndex[i] = -1
			}
			rf.resetTimer()
		}
		return 
	} 
}

// 2B
func (rf *Raft) sendAppendEntriesToFollower(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesToAllFollower() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := AppendEntryArgs{
			Term:			rf.currentTerm,
			LeaderId: 		rf.me,
			PrevLogIndex: 	rf.nextIndex[i]-1,
		}
		if args.PrevLogIndex >= 0 {
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		}
		if rf.nextIndex[i] < len(rf.logs) {
			args.Entries = rf.logs[rf.nextIndex[i]:]
		}
		args.LeaderCommit = rf.CommandIndex

		go func(server int, args AppendEntryArgs, rf *Raft) {
			var reply = AppendEntryReply

			retry:
			
			if rf.currentState != LEADER {
				return
			}
			ok := rf.sendAppendEntriesToFollower(server, args, &reply)
			if ok {
				rf.handleAppendEntries(server, reply)
			}else {
				goto retry
			}(i, args, rf)
		}
	}
}
