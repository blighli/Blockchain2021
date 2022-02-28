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
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
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

type LogEntry struct {
	Command interface{}
	Index   int
	Term    int
}

type raftError struct {
	msg string
}

func (re *raftError) Error() string {
	return re.msg
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

	//选举附加状态
	//当前角色 0 follower 1 candidate 2 leader
	role int

	//选举开始时间及时间间隔
	electionStartTime time.Time
	electionTimeOut   time.Duration

	//得到票数
	voteCount int
	applyCh   chan ApplyMsg

	//Persistent state on all servers:
	currentTerm int
	votedFor    int
	logEntries  []LogEntry

	//Volatile state on all servers:
	commitIndex int
	lastApplied int

	//Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	//为snapshot持久化的字段，上次snapshot最后一条日志的index和term
	lastIncludedTerm  int
	lastIncludedIndex int
}

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == 2
	return term, isleader
}

//snapshot为空只持久化state
//snapshot不为空持久化state和snapshot
func (rf *Raft) persist(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	if snapshot != nil {
		rf.persister.SaveStateAndSnapshot(data, snapshot)
	} else {
		rf.persister.SaveRaftState(data)
	}
}

func (rf *Raft) readPersist(data []byte) error {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return nil
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logEntries []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil {
		rf.currentTerm = 0
		rf.votedFor = -1
		return &raftError{msg: "读取持久化数据失败"}
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logEntries = logEntries
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		return nil
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	//todo 论文的表述是Raft has processed entries after the snapshot's lastIncludedTerm/lastIncludedIndex
	if lastIncludedIndex < rf.lastIncludedIndex || len(rf.logEntries) != 0 && lastIncludedIndex < rf.logEntries[len(rf.logEntries)-1].Index {
		return false
	}

	rf.Snapshot(lastIncludedIndex, snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	//丢弃原有logEntries
	if len(rf.logEntries) == 0 {
		return
	}

	//这里会死锁，不知道原因
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	logEntries := make([]LogEntry, 0)
	for i, entry := range rf.logEntries {
		if entry.Index == index {
			//丢弃日志时保存最后一条entry的index和term
			rf.lastIncludedIndex = entry.Index
			rf.lastIncludedTerm = entry.Term
			for j := i + 1; j < len(rf.logEntries); j++ {
				logEntries = append(logEntries, rf.logEntries[j])
			}
		}
	}
	rf.logEntries = logEntries
	// fmt.Printf("rf:%v logEntries:%v lastIncludedIndex:%v\n", rf.me, rf.logEntries, rf.lastIncludedIndex)

	rf.persist(snapshot)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LogEntries   []LogEntry
	LeaderCommit int
}

type AppednEntriesReply struct {
	Term     int
	Success  bool
	FailInfo FailInfo
}

type FailInfo struct {
	Term  int
	Index int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppednEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateElectionTime()
	if args.Term > rf.currentTerm {
		rf.convertToFllower(args.Term)
	}

	//失败但不处理nextIndex[i],用reply.FailInfo.Term=-1来标识
	if args.Term < rf.currentTerm || args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.FailInfo.Term = -1
		return
	}

	/*
		Figure8优化

		比较args.PrevLogIndex与rf.lastIncludedIndex
		- args.PrevLogIndex<rf.lastIncludedIndex,失败但不处理nextIndex[i],用reply.FailInfo.Term=-1来标识,直接return

		- args.PrevLogIndex==rf.lastIncludedIndex，比较args.PrevLogTerm与rf.lastIncludedTerm
		-- args.PrevLogTerm==rf.lastIncludedTerm(应该只有这一种情况),成功

		- args.PrevLogIndex>rf.lastIncludedIndex,比较args.PrevLogIndex-rf.lastIncludedIndex-1与len(rf.logEntries)-1
		-- args.PrevLogIndex-rf.lastIncludedIndex-1>len(rf.logEntries)-1,失败,failIndex=len(rf.logEntries)-1
		-- args.PrevLogIndex-rf.lastIncludedIndex-1<=len(rf.logEntries)-1,比较rf.logEntries[args.PrevLogIndex-rf.lastIncludedIndex-1].Term与args.PrevLogTerm
		--- rf.logEntries[args.PrevLogIndex-rf.lastIncludedIndex-1].Term==args.PrevLogTerm,成功
		--- rf.logEntries[args.PrevLogIndex-rf.lastIncludedIndex-1].Term!=args.PrevLogTerm,失败,failIndex=args.PrevLogIndex-rf.lastIncludedIndex-1


		fail阶段
		从failIndex开始向前遍历到[1]，若term与前一条不同则该entry为falseReturnEntry，break；若到头才结束则判断[0].term和lastIncludedTerm是否相同
		- 相同则设falseReturnEntry.index=lastIncludedIndex,falseReturnEntry.term=lastIncludedTerm
		- 不相同则设falseReturnEntry=rf.logEntries[0]
		设置FailInfo.index=falseReturnEntry.index,FailInfo.term=falseReturnEntry.term


		appendEntries阶段
		修改rf.logEntries
		修改rf.commitIndex

	*/
	success := true
	failIndex := -1
	failReturnEntry := LogEntry{}

	if args.PrevLogIndex != -1 && args.PrevLogTerm != -1 {
		if args.PrevLogIndex > rf.lastIncludedIndex {
			if args.PrevLogIndex-rf.lastIncludedIndex-1 > len(rf.logEntries)-1 {
				success = false
				failIndex = len(rf.logEntries) - 1

			} else {
				if rf.logEntries[args.PrevLogIndex-rf.lastIncludedIndex-1].Term != args.PrevLogTerm {
					success = false
					failIndex = args.PrevLogIndex - rf.lastIncludedIndex - 1
				}
			}
		}
	}

	if success {
		//更新logEntries
		rf.logEntries = append(rf.logEntries[:args.PrevLogIndex-rf.lastIncludedIndex-1+1], args.LogEntries...)
		rf.persist(nil)

		//更新commitIndex
		if args.LeaderCommit > rf.commitIndex {
			if len(args.LogEntries) == 0 || args.LeaderCommit <= args.LogEntries[len(args.LogEntries)-1].Index {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = args.LogEntries[len(args.LogEntries)-1].Index
			}
			if rf.commitIndex > rf.lastApplied {
				rf.updateLastApplied()
			}
		}

		reply.Success = true
		reply.Term = rf.currentTerm
		return
	} else {
		for ; failIndex >= 1; failIndex-- {
			if rf.logEntries[failIndex-1].Term != rf.logEntries[failIndex].Term {
				failReturnEntry = rf.logEntries[failIndex]
				break
			}
		}
		if failIndex == 0 {
			if rf.logEntries[0].Term != rf.lastIncludedTerm {
				failReturnEntry = rf.logEntries[0]
			} else {
				failReturnEntry = LogEntry{
					Index: rf.lastIncludedIndex,
					Term:  rf.lastIncludedTerm,
				}
			}
		}
		if failIndex == -1 {
			failReturnEntry = LogEntry{
				Index: rf.lastIncludedIndex,
				Term:  rf.lastIncludedTerm,
			}
		}
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.FailInfo.Index = failReturnEntry.Index
		reply.FailInfo.Term = failReturnEntry.Term
		return
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppednEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) handleSendAppendEntries(server int, args *AppendEntriesArgs, reply *AppednEntriesReply) {
	ok := rf.sendAppendEntries(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.convertToFllower(reply.Term)
			return
		}
		if reply.Success {
			if len(args.LogEntries) != 0 {
				rf.nextIndex[server] = args.LogEntries[len(args.LogEntries)-1].Index + 1
				rf.matchIndex[server] = args.LogEntries[len(args.LogEntries)-1].Index
			}
		} else {
			if reply.FailInfo.Term != -1 {
				rf.nextIndex[server] = reply.FailInfo.Index
			}
		}
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.convertToFllower(args.Term)
	}
	//投票
	if rf.votedFor == -1 && (len(rf.logEntries) == 0 ||
		(args.LastLogTerm > rf.logEntries[len(rf.logEntries)-1].Term || args.LastLogTerm == rf.logEntries[len(rf.logEntries)-1].Term && args.LastLogIndex >= rf.logEntries[len(rf.logEntries)-1].Index)) {
		rf.votedFor = args.CandidateId
		rf.persist(nil)
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.updateElectionTime()
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) handleSendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.sendRequestVote(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.convertToFllower(reply.Term)
			return
		}
		if reply.VoteGranted && reply.Term == rf.currentTerm {
			rf.voteCount++
		}
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.convertToFllower(args.Term)
		rf.mu.Unlock()

	}

	//todo 这个判断与论文中略有出入，待测试是否准确
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		return
	}

	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	reply.Term = rf.currentTerm

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) handleSendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.sendInstallSnapshot(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.convertToFllower(reply.Term)
			return
		}
	}
}

//启动，包括失败重启
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.role = 0
	rf.voteCount = 0
	rf.updateElectionTime()

	// initialize from state persisted before a crash
	if rf.readPersist(persister.ReadRaftState()) != nil {
		fmt.Printf("初始化错误!")
		return nil
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.leaderTicker()

	return rf
}

//开始一次共识
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.role == 2
	if isLeader {
		term = rf.currentTerm
		if len(rf.logEntries) > 0 {
			index = rf.logEntries[len(rf.logEntries)-1].Index + 1
		} else {
			index = rf.lastIncludedIndex + 1
		}
		entry := LogEntry{
			Command: command,
			Term:    term,
			Index:   index,
		}
		rf.logEntries = append(rf.logEntries, entry)
		rf.persist(nil)

		// fmt.Printf("newEntry:%v\n", entry)

		//更新自己的matchIndex、nextIndex
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
	}
	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.role == 0 || rf.role == 1 {

			//start a new election
			if time.Since(rf.electionStartTime) >= rf.electionTimeOut {
				rf.convertToCandidate()
			}

			//get a majority of the votes
			if rf.voteCount > len(rf.peers)/2 {
				rf.convertToLeader()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) leaderTicker() {
	for !rf.killed() {
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
		if rf.role == 2 {
			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				//发送append entries
				appendEntriesArgs := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
				}

				// - 如果nextIndex[i]<lastIncludedIndex+1
				//   - 调用AppendEntries单纯当作心跳信号(将args.PrevLogIndex、args.PrevLogTerm设为-1)
				//   - 调用InstallSnapshot下载snapshot
				// - 如果nextIndex[i]==lastIncludedIndex+1
				//   - 调用AppendEntries，PrevLogIndex[i]与term设置成lastIncludedIndex和term
				// - 如果nextIndex[i]>lastIncludedIndex+1
				//	 - 在logEntries中寻找index==nextIndex[i]-1

				if rf.nextIndex[i] < rf.lastIncludedIndex+1 {
					appendEntriesArgs.PrevLogIndex = -1
					appendEntriesArgs.PrevLogTerm = -1

					installSnapshotArgs := InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm:  rf.lastIncludedTerm,
					}
					// fmt.Printf("snapshot:%v\n", "snapshot")

					snapshot := rf.persister.ReadSnapshot()
					if snapshot != nil {
						installSnapshotArgs.Snapshot = snapshot
					}
					installSnapshotReply := InstallSnapshotReply{}
					go rf.handleSendInstallSnapshot(i, &installSnapshotArgs, &installSnapshotReply)

				} else {
					if len(rf.logEntries) != 0 && rf.logEntries[len(rf.logEntries)-1].Index >= rf.nextIndex[i] {
						appendEntriesArgs.LogEntries = rf.logEntries[rf.nextIndex[i]-rf.lastIncludedIndex-1:]
					}

					if rf.nextIndex[i] > rf.lastIncludedIndex+1 {
						for _, logEntry := range rf.logEntries {
							if logEntry.Index == rf.nextIndex[i]-1 {
								appendEntriesArgs.PrevLogIndex = logEntry.Index
								appendEntriesArgs.PrevLogTerm = logEntry.Term
								break
							}
						}
					} else if rf.nextIndex[i] == rf.lastIncludedIndex+1 {
						appendEntriesArgs.PrevLogIndex = rf.lastIncludedIndex
						appendEntriesArgs.PrevLogTerm = rf.lastIncludedTerm
					}
				}

				appendEntriesReply := AppednEntriesReply{}
				go rf.handleSendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply)

				//更新commitIndex 将match进行排序，找到大于等于中间的那一个matchIndex，作为新的commitIndex（即大多数节点commit过的index）
				sortMatch := append(make([]int, 0), rf.matchIndex...)
				sort.Sort(sort.Reverse(sort.IntSlice(sortMatch)))
				N := sortMatch[len(rf.peers)/2]
				if N > rf.commitIndex && rf.logEntries[N-rf.lastIncludedIndex-1].Term == rf.currentTerm {
					rf.commitIndex = N
				}

				//更新lastApplied
				if rf.commitIndex > rf.lastApplied {
					rf.updateLastApplied()
				}
			}

		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) updateLastApplied() {
	//由于在传递ApplyMsg时有可能触发SnapShot造成rf.logEntries变化，因此要先组装待传的command在往信道中传递
	applyMsgs := make([]ApplyMsg, 0)
	for i := rf.lastApplied; i < rf.commitIndex; i++ {
		// fmt.Printf("i: %v\n", i)
		applyMsgs = append(applyMsgs, ApplyMsg{
			CommandValid: true,
			CommandIndex: i + 1,
			Command:      rf.logEntries[i+1-rf.lastIncludedIndex-1].Command,
		})

	}
	for _, msg := range applyMsgs {
		rf.applyCh <- msg
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) updateElectionTime() {
	rf.electionStartTime = time.Now()
	rf.electionTimeOut = 500*time.Millisecond + time.Duration(rand.Intn(500))*time.Millisecond
}

func (rf *Raft) convertToFllower(term int) {
	rf.role = 0
	rf.currentTerm = term
	rf.votedFor = -1
	rf.voteCount = 0
	rf.persist(nil)
}

func (rf *Raft) convertToCandidate() {
	rf.role = 1
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.updateElectionTime()
	rf.voteCount = 1
	for i := range rf.peers {
		if i != rf.me {
			args := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			if len(rf.logEntries) != 0 {
				args.LastLogIndex = rf.logEntries[len(rf.logEntries)-1].Index
				args.LastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
			}
			reply := RequestVoteReply{}
			go rf.handleSendRequestVote(i, &args, &reply)
		}
	}
	//todo 这个调用的位置有点奇怪，改到循环上面测一下
	rf.persist(nil)
}

func (rf *Raft) convertToLeader() {
	rf.role = 2

	matchIndex := rf.lastIncludedIndex
	nextIndex := rf.lastIncludedIndex + 1

	if len(rf.logEntries) != 0 {
		nextIndex = rf.logEntries[len(rf.logEntries)-1].Index + 1
	}

	rf.matchIndex = make([]int, 0)
	rf.nextIndex = make([]int, 0)
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex = append(rf.matchIndex, matchIndex)
		rf.nextIndex = append(rf.nextIndex, nextIndex)
	}
}
