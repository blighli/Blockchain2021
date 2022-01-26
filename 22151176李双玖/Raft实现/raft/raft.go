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

	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

const (
	// 节点状态
	followerState  = "follower"
	candidateState = "candidate"
	leaderState    = "leader"

	// 节点选举超时时间：random(electionTimeoutMin ms, electionTimeoutMax ms)
	electionTimeoutMin = 300
	electionTimeoutMax = 600
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

	// 需持久化保存的变量
	// why votedFor?
	//	to prevent a client from voting for one candidate, then reboot,
	// 	then vote for a different candidate in the same (or older!) term
	// 	could lead to two leaders for the same term
	curTerm           int32       // 当前任期，初始为0
	votedFor          int32       // 投票给的节点索引，还未投票则为-1
	logs              []*LogEntry // 日志，索引从1开始，初始化第0项为无效条目
	lastIncludedIndex int32       // 快照的最后一个条目的index值，初始化0
	lastIncludedTerm  int32       // 快照的最后一个条目的term值，初始化0
	snapshotData      []byte      // 快照

	discoverLeader     bool          // 选举超时时间内，发现合法leader，初始化false
	state              string        // 状态：follower, candidate, leader
	lastCommittedIndex int32         // 最后一条已提交的日志条目，从1开始，初始化为0
	lastAppliedIndex   int32         // 最后一条被执行的日志条目，从1开始，初始化为0
	applyCh            chan ApplyMsg // 状态机接口

	// leader 状态时使用的变量
	nextIndex  []int32 // 发送给每个节点的下一个条目的索引，初始化都为 lastLogIndex + 1
	matchIndex []int32 // 与每个节点相匹配日志条目的最大索引，初始化都为 0
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.curTerm), rf.state == leaderState
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	buffer := new(bytes.Buffer)
	e := labgob.NewEncoder(buffer)
	e.Encode(rf.curTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs) // 索引1开始的才是有效条目
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	stateData := buffer.Bytes()
	// rf.persister.SaveRaftState(data)
	rf.persister.SaveStateAndSnapshot(stateData, rf.snapshotData)
	DPrintf("[%d][term %d][%s] persist curTerm:%d, votedFor:%d, lastIncludedIndex:%d, lastIncludedTerm:%d", rf.me, rf.curTerm, rf.state, rf.curTerm, rf.votedFor, rf.lastIncludedIndex, rf.lastIncludedTerm)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist() {
	stateData := rf.persister.ReadRaftState()
	// 初次启动，无数据
	if stateData == nil || len(stateData) < 1 { // bootstrap without any state?
		return
	}
	buffer := bytes.NewBuffer(stateData)
	d := labgob.NewDecoder(buffer)
	if d.Decode(&rf.curTerm) != nil || d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.logs) != nil ||
		d.Decode(&rf.lastIncludedIndex) != nil || d.Decode(&rf.lastIncludedTerm) != nil {
		log.Fatal("readPersist error")
	}
	// Note: 读取快照数据之后，需更新 lastAppliedIndex 和 lastCommittedIndex
	rf.lastAppliedIndex = rf.lastIncludedIndex
	rf.lastCommittedIndex = rf.lastIncludedIndex
	rf.snapshotData = rf.persister.ReadSnapshot()
	DPrintf("[%d][term %d][%s] readPersist curTerm:%d, votedFor:%d, lastIncludedIndex:%d, lastIncludedTerm:%d", rf.me, rf.curTerm, rf.state, rf.curTerm, rf.votedFor, rf.lastIncludedIndex, rf.lastIncludedTerm)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	DPrintf("[%d][term %d][%s] CondInstallSnapshot: rf.lastIncludedIndex(%d) -> lastIncludedIndex(%d)", rf.me, rf.curTerm, rf.state, rf.lastIncludedIndex, lastIncludedIndex)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex <= int(rf.lastIncludedIndex) {
		return false
	}

	// 必须按照如下顺序
	rf.lastIncludedTerm = int32(lastIncludedTerm)
	if lastIncludedIndex > int(rf.getLastLogIndex()) {
		// 快照最后一个索引超过日志最后一个索引
		rf.logs = make([]*LogEntry, 0)
	} else {
		rf.logs = rf.logs[rf.getActLogIndex(int32(lastIncludedIndex)+1):]
	}
	rf.lastIncludedIndex = int32(lastIncludedIndex)
	rf.snapshotData = snapshot
	// Note: 需更新 lastAppliedIndex 和 lastCommittedIndex
	if rf.lastCommittedIndex < int32(lastIncludedIndex) {
		rf.lastCommittedIndex = int32(lastIncludedIndex)
	}
	if rf.lastAppliedIndex < int32(lastIncludedIndex) {
		rf.lastAppliedIndex = int32(lastIncludedIndex)
	}
	rf.persist()
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	DPrintf("[%d][term %d][%s] get new Snapshot, rf.lastIncludedIndex(%d) -> index(%d)", rf.me, rf.curTerm, rf.state, rf.lastIncludedIndex, index)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index == int(rf.lastIncludedIndex) {
		return
	}
	// 必须按照如下顺序
	rf.lastIncludedTerm = rf.logs[rf.getActLogIndex(int32(index))].Term
	rf.logs = rf.logs[rf.getActLogIndex(int32(index)+1):]
	// rf.getActLogIndex 会使用 rf.lastIncludedIndex ，它必须在最后更新
	rf.lastIncludedIndex = int32(index)
	rf.snapshotData = snapshot
	rf.persist()
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leaderState {
		return -1, -1, false
	}
	rf.logs = append(rf.logs, &LogEntry{
		Index:   rf.getLastLogIndex() + 1,
		Term:    rf.curTerm,
		Command: command,
	})
	rf.persist()
	DPrintf("[%d][term %d][%s] get new LogEntry: %+v", rf.me, rf.curTerm, rf.state, rf.logs)
	return int(rf.getLastLogIndex()), int(rf.curTerm), true
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
	DPrintf("[%d][term %d][%s] Killed", rf.me, rf.curTerm, rf.state)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// 等待随机时间，防止初次启动同时成为candidate
	electionTimeout := rand.Int31n(electionTimeoutMax-electionTimeoutMin) + electionTimeoutMin
	DPrintf("[%d][term %d][%s] wait electionTimeout:%d", rf.me, rf.curTerm, rf.state, electionTimeout)
	time.Sleep(time.Duration(electionTimeout) * time.Millisecond)

	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// 随机化选举超时时间
		electionTimeout = rand.Int31n(electionTimeoutMax-electionTimeoutMin) + electionTimeoutMin

		DPrintf("[%d][term %d][%s] discoverLeader:%v, votedFor:%d", rf.me, rf.curTerm, rf.state, rf.discoverLeader, rf.votedFor)

		rf.mu.Lock()
		// 超时时间内未成为leader，也没有发现其它leader，则成为候选人开始新一轮的选举
		if rf.state != leaderState && !rf.discoverLeader {
			// 另起线程，防止阻塞当前定时器线程
			go rf.becomeCandidate()
		}
		rf.discoverLeader = false // 重置
		rf.mu.Unlock()

		DPrintf("[%d][term %d][%s] wait electionTimeout:%d", rf.me, rf.curTerm, rf.state, electionTimeout)
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
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
	rf.curTerm = 0
	rf.logs = make([]*LogEntry, 0)
	// 第0项为无效条目
	// rf.logs = append(rf.logs, &LogEntry{
	// 	Index:   0,
	// 	Term:    -1,
	// 	Command: nil,
	// })
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.lastCommittedIndex = 0
	rf.lastAppliedIndex = 0
	rf.applyCh = applyCh
	rf.discoverLeader = false
	rf.becomeFollower() // 不管是初次启动，还是宕机后重启，都是follower状态

	// initialize from state persisted before a crash
	rf.readPersist()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
