package raft

// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Entry struct {
	Command interface{}
	Term    int
}

const (
	Follower = iota
	Leader
	Candidate
)

// 与时间相关的常数，以毫秒为单位
const (
	FixedTimeout       = 200
	RandomTimeout      = 200
	HeartbeatPeriod    = 100
	CheckTimeoutPeriod = 20
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers:
	currentTerm int     // latest Term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int     //	CandidateId that received vote in current Term (or null if none)
	log         []Entry // log entries; each entry contains command for state machine, and Term when entry was received by leader (first index is 1)

	// volatile state on all servers:
	commitIndex   int           // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied   int           // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	state         int           // each server has three state: Follower/Leader/Candidate
	timer         Timer         // time a server, if time-out, then convert to candidate and kick off an election
	applyCh       chan ApplyMsg // channel to send message to application
	newApplicable *sync.Cond    // condition variable used to wake goroutine that apply committed entries

	// volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

}

type AppendEntriesArgs struct {
	Term         int     // leader’s term
	LeaderId     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// additional information needed to back up faster
	ConflictTerm           int // term of the conflicting entry
	FirstConflictTermIndex int // index of the first entry of conflicting term
	LengthLog              int // length of log
}

type RequestVoteArgs struct {
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // Term of candidate’s last log entry
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type Timer struct {
	startTime time.Time
	timeout   time.Duration
	r         *rand.Rand
}

/**
 * @Description: 判断raft服务器的时钟是否超时
 * @return bool raft服务器的时钟是否超时
 */
func (t *Timer) isExpired() bool {
	return time.Now().Sub(t.startTime) > t.timeout
}

/**
 * @Description: 初始化raft服务器的超时时钟，如果超时，则转换为Candidate并开始选举
 */
func (t *Timer) reset() {
	t.timeout = FixedTimeout*time.Millisecond +
		time.Duration(t.r.Int63n(RandomTimeout))*time.Millisecond
	t.startTime = time.Now()
}

// GetState
/**
 * @Description: 返回当前任期以及该服务器是否认为它是leader
 * @return int 当前任期
 * @return bool 该服务器是否认为它是leader
 */
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.state == Leader
	return term, isLeader
}

/**
 * @Description: 将 Raft 的持久状态保存到稳定的存储中，以后可以在故障和重新启动后检索它
 */
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		log.Fatalf("[%d] fails to encode currentTerm", &rf.me)
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		log.Fatalf("[%d] fails to encode votedFor", &rf.me)
	}
	err = e.Encode(rf.log)
	if err != nil {
		log.Fatalf("[%d] fails to encode log", &rf.me)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

/**
 * @Description: 恢复以前保存的持久状态
 * @param data 以前保存的持久状态
 */
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var persistedLog []Entry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&persistedLog) != nil {
		// fail to read persistent data
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = persistedLog
	}
}

// RequestVote
/**
 * @Description: Candidate调用RequestVote RPC以收集选票
 * @param args RequestVote RPC的参数
 * @param reply RequestVote RPC的回复
 */
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.LogTerm(len(rf.log)) ||
			args.LastLogTerm == rf.LogTerm(len(rf.log)) && args.LastLogIndex >= len(rf.log)) {
		rf.state = Follower
		rf.timer.reset()
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
	}
}

// LogTerm
/**
 * @Description: 返回给定日志条目索引的任期
 * @param index 日志索引(从1开始)
 * @return int 给定日志条目索引的任期
 */
func (rf *Raft) LogTerm(index int) int {
	// 如果索引超出下限，则返回 -1
	if index-1 < 0 {
		return -1
	}
	return rf.log[index-1].Term
}

// AppendEntries
/**
 * @Description: leader调用AppendEntries RPC以复制日志条目，也用作心跳
 * @param args AppendEntries RPC参数
 * @param reply AppendEntries RPC回复
 */
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.LengthLog = -1
	reply.ConflictTerm = -1

	if args.Term < rf.currentTerm {
		return
	}

	// 这是一个合法的领导者，因为 term >= currentTerm，因此重置计时器
	rf.timer.reset()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = args.LeaderId
		rf.persist()
	} else {
		rf.state = Follower
	}

	if len(rf.log) < args.PrevLogIndex || rf.LogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.LengthLog = len(rf.log)
		if len(rf.log) >= args.PrevLogIndex {
			reply.ConflictTerm = rf.LogTerm(args.PrevLogIndex)
			reply.FirstConflictTermIndex = rf.firstIndex(reply.ConflictTerm)
		}
		return
	}

	index := 0
	for index < len(args.Entries) && args.PrevLogIndex+index < len(rf.log) {
		if rf.log[args.PrevLogIndex+index].Term != args.Entries[index].Term {
			break
		}
		index++
	}
	args.Entries = args.Entries[index:]

	if len(args.Entries) > 0 {
		rf.log = rf.log[:args.PrevLogIndex+index]
		rf.log = append(rf.log, args.Entries...)
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.log) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log)
		}
		rf.newApplicable.Signal()
	}

	reply.Success = true
}

/**
 * @Description: 找到给定任期的第一个日志条目索引
 * @param term 给定任期
 * @return int 给定任期的第一个索引
 */
func (rf *Raft) firstIndex(term int) int {
	left, right, res := 0, len(rf.log)-1, -1
	for left <= right {
		mid := left + (right-left)/2
		if rf.log[mid].Term < term {
			left = mid + 1
		} else if rf.log[mid].Term > term {
			right = mid - 1
		} else {
			res = mid
			right = mid - 1
		}
	}
	return res + 1
}

/**
 * @Description: 查找给定任期的最后一个日志条目索引
 * @param term 给定任期
 * @return int 给定任期的最后一个索引
 */
func (rf *Raft) lastIndex(term int) int {
	left, right, res := 0, len(rf.log)-1, -1
	for left <= right {
		mid := left + (right-left)/2
		if rf.log[mid].Term < term {
			left = mid + 1
		} else if rf.log[mid].Term > term {
			right = mid - 1
		} else {
			res = mid
			left = mid + 1
		}
	}
	return res + 1
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
/**
 * @Description: 使用 Raft 的服务（例如 k/v 服务器）希望就下一个要附加到 Raft 日志的命令达成一致。如果此服务器不是leader，则
 * 返回 false。否则启动共识并立即返回。无法保证此命令将永远提交到 Raft 日志，因为leader可能会故障或在选举中失败。即使 Raft 实例
 * 被杀死，这个函数也应该优雅地返回
 * @param command 下一个要附加到Raft日志的命令
 * @return int 该命令在提交时将出现的索引
 * @return int 当前任期
 * @return bool 如果此服务器认为它是leader，则返回true
 */
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	rf.mu.Lock()

	if rf.state != Leader || rf.killed() {
		rf.mu.Unlock()
		return index, term, false
	}

	index = len(rf.log) + 1
	term = rf.currentTerm

	// 将新条目附加到领导者
	e := Entry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, e)
	rf.persist()
	rf.mu.Unlock()

	rf.callAppendEntries()
	return index, term, true
}

/**
 * @Description: 获取已知已提交的日志条目的最高索引。调用者在调用此函数时应持有锁rf.mu
 * @return int
 */
func (rf *Raft) maxCommitIndex() int {
	maxCommit := rf.commitIndex
	next := rf.commitIndex + 1
	for rf.canCommit(next) {
		if rf.LogTerm(next) == rf.currentTerm {
			maxCommit = next
		}
		next++
	}
	return maxCommit
}

/**
 * @Description: 检查是否允许提交entry[0...index]
 * @param index 日志条目的最后一个索引
 * @return bool 是否允许提交
 */
func (rf *Raft) canCommit(index int) bool {
	if index > len(rf.log) {
		return false
	}
	// 计算具有 log[index] 的服务器
	count := 1
	for i, n := range rf.matchIndex {
		if i == rf.me {
			continue
		}
		if n >= index {
			count++
		}
	}
	return count > len(rf.peers)/2
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
/**
 * @Description: 创建一个Raft服务器。Make()必须快速返回，因此它应该为任何长时间运行
 * 的工作启动 goroutine。为每个新提交的日志条目发送一个 ApplyMsg 到 Make() 的 applyCh 通道参数
 * @param peers       所有 Raft 服务器（包括这个）的端口，所有服务器的 peers[] 数组都具有相同的顺序。
 * @param me          peers[me]代表此服务器的端口
 * @param persister   此raft服务器保存的持久状态
 * @param applyCh     Raft发送 ApplyMsg 消息的通道
 * @return *Raft
 */
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.newApplicable = sync.NewCond(&rf.mu)
	rf.timer = Timer{startTime: time.Now(), r: rand.New(rand.NewSource(int64(me + 1)))}
	rf.state = Follower
	rf.timer.reset()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 定期检查是否超时
	go rf.periodicTimeout()
	// 检查是否有任何新提交的条目要应用
	go rf.applyCommittedEntries()

	return rf
}

/**
 * @Description: Candidate调用RequestVote RPC以收集选票
 * @param server 接收投票请求的节点索引
 * @param args RequestVote RPC的参数
 * @param reply RequestVote RPC的回复
 * @return bool 是否发送成功
 */
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

/**
 * @Description: leader调用AppendEntries RPC以复制日志条目，也用作心跳
 * @param server follower节点的索引
 * @param args AppendEntries RPC参数
 * @param reply AppendEntries RPC回复
 * @return bool 是否发送成功
 */
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

/**
 * @Description: 定期检查节点是否超时
 */
func (rf *Raft) periodicTimeout() {
	for !rf.killed() {
		rf.mu.Lock()
		// 计时器超时时不是领导者，转换为候选人并开始选举
		if rf.state != Leader && rf.timer.isExpired() {
			go rf.kickOffElection()
		}
		rf.mu.Unlock()
		time.Sleep(CheckTimeoutPeriod * time.Millisecond)
	}
}

/**
 * @Description: leader定期向其它节点发送心跳维护权威
 */
func (rf *Raft) periodicHeartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			rf.callAppendEntries()
		} else {
			// 不再是领导者，退出这个goroutine
			rf.mu.Unlock()
			return
		}
		time.Sleep(HeartbeatPeriod * time.Millisecond)
	}
}

/**
 * @Description: 计时器超时，如果节点不是leader，转换为Candidate并发起选举
 */
func (rf *Raft) kickOffElection() {
	rf.mu.Lock()
	rf.timer.reset()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	term := rf.currentTerm
	voteCount := 1
	done := false
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			voteGranted := rf.callRequestVote(server, term)
			if !voteGranted {
				return
			}
			// 收到投票
			rf.mu.Lock()
			voteCount++
			// 如果这个 goroutine 发现没有足够的投票，则退出
			if done || voteCount <= len(rf.peers)/2 {
				rf.mu.Unlock()
				return
			}
			// 获得足够的选票，成为领导者
			done = true
			// 再次检查它是否仍在选举开始时的任期内
			if rf.currentTerm == term {
				rf.initializeLeader()
				rf.mu.Unlock()
				// 启动一个 goroutine 定期发送心跳
				go rf.periodicHeartbeat()
			} else {
				rf.mu.Unlock()
			}
		}(i)
	}
}

/**
 * @Description: Candidate获得过半投票，成为Leader，调用该函数初始化Candidate为Leader状态
 * 调用者在调用此函数时应持有锁rf.mu
 */
func (rf *Raft) initializeLeader() {
	rf.state = Leader
	// nextIndex初始化为日志的末尾索引
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.log) + 1
	}

	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0
	}
}

/**
 * @Description: 由于在 applyCh 上发送可能会阻塞，因此必须在单个 goroutine 中完成。 这个 goroutine 在向 rf.applyCh 发送消息时
 * 不应该持有任何锁
 */
func (rf *Raft) applyCommittedEntries() {
	for !rf.killed() {
		rf.newApplicable.L.Lock()
		// 检查是否有任何新的可以应用的条目
		for rf.lastApplied >= rf.commitIndex {
			rf.newApplicable.Wait()
		}

		var messages []ApplyMsg
		// 应用提交的条目
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i-1].Command,
				CommandIndex: i,
			}
			messages = append(messages, msg)
		}

		rf.newApplicable.L.Unlock()

		for _, m := range messages {
			rf.applyCh <- m
			rf.mu.Lock()
			rf.lastApplied++
			rf.mu.Unlock()
		}
	}
}

/**
 * @Description: leader向每个节点发送AppendEntries RPC，这个RPC也可以用于实现心跳机制
 */
func (rf *Raft) callAppendEntries() {
	rf.mu.Lock()
	// 每次发送心跳前更新leader的commitIndex
	commitIndex := rf.maxCommitIndex()
	// 有新提交的条目，唤醒 goroutine applyCommittedEntries 以应用这些条目
	if commitIndex != rf.commitIndex {
		rf.commitIndex = commitIndex
		rf.newApplicable.Signal()
	}
	me := rf.me
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == me {
			continue
		}
		rf.mu.Lock()
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm:  rf.LogTerm(rf.nextIndex[i] - 1),
			Entries:      rf.log[rf.nextIndex[i]-1:],
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		rf.mu.Unlock()
		go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
			ok := rf.sendAppendEntries(server, args, reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			// 回复包含更高的任期，转换为follower
			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
			} else if !reply.Success && reply.LengthLog != -1 {
				if reply.LengthLog < args.PrevLogIndex {
					rf.nextIndex[server] = reply.LengthLog + 1
				} else if lastIndex := rf.lastIndex(reply.ConflictTerm); lastIndex != -1 {
					rf.nextIndex[server] = lastIndex + 1
				} else {
					// 领导者根本没有回复的任期
					rf.nextIndex[server] = reply.FirstConflictTermIndex
				}
			} else {
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			}
			rf.mu.Unlock()
		}(i, &args, &reply)
	}
}

/**
 * @Description: Candidate向其它节点发起投票请求
 * @param server 接收投票请求的节点索引
 * @param term Candidate的当前任期
 * @return bool 该节点是否投票给该Candidate
 */
func (rf *Raft) callRequestVote(server int, term int) bool {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log),
		LastLogTerm:  rf.LogTerm(len(rf.log)),
	}
	rf.mu.Unlock()
	var reply RequestVoteReply

	ok := rf.sendRequestVote(server, &args, &reply)

	if !ok {
		return false
	}
	rf.mu.Lock()
	// 回复包含更高的术语，转换为follower
	if rf.currentTerm < reply.Term {
		rf.convertToFollower(reply.Term)
	}
	rf.mu.Unlock()
	return reply.VoteGranted
}

/**
 * @Description: 转换为follower
 * @param term 当前任期
 */
func (rf *Raft) convertToFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.timer.reset()
	rf.persist()
}
