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
	"6.824/labgob"
	"bytes"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// 常量的设置，包括状态对应的int，以及timeout时间
const (
	FOLLOWER = 0
	CANDIDATE = 1
	LEADER = 2

	// 设置选举时间的随机区间
	ELECTION_TIMEOUT_MAX = 400
	ELECTION_TIMEOUT_MIN = 200

	// 设置心跳的timeout
	HEARTBEAT_TIMEOUT = 100
	// 设置apply的timeout
	APPLY_TIMEOUT = 120
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

// 定义Entry的数据结构
type Entry struct {
	//哪个Term
	Term int
	//什么Command
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
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 对照state表
	currentTerm int //当前看到的最新term
	votedFor int //给哪个candidate投票
	log []Entry //log entries
	getVoteNum int //获得多少的票

	commitIndex int //最新的committed的log entry
	lastApplied int //最新的applied给state machine的log entry

	state int // 当前的角色
	lastElectionTime time.Time // 上次重新选举的时间

	nextIndex []int // leader记录的发给每个server的next log entry的index
	matchIndex []int // leader记录的每个server复制的最高index

	applyCh chan ApplyMsg // 结构体使用chan，由leader返回给tester



}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//查询状态前还是先lock掉
	//否则查着查着改了怎么办
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// 将state的头三个编码并保存起来
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var p_currentTerm int
	var p_voteFor int
	var p_log []Entry

	// 返回的是error，nil就是没有error
	if d.Decode(&p_currentTerm) != nil ||
	    d.Decode(&p_voteFor) != nil ||
		d.Decode(&p_log) != nil {
		DPrintf("%d read persister wrong!", rf.me)
		} else {
	   rf.currentTerm = p_currentTerm
	   rf.votedFor = p_voteFor
	   rf.log = p_log
	 }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//



func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// 能过就行
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// 只保留index后面的（不包括index）
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// rpc的field name都要大写才可以
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 锁一下
	rf.mu.Lock()
	defer rf.mu.Unlock()


	// Reply false if term < currentTerm
	// 若candidate的term比当前rf的currentTerm小
	if args.Term < rf.currentTerm {
		DPrintf("[Violate_RequestVote_RPC_rule_1]Server %d reject %d, myterm %d, candidate term %d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		// 不给candidate投票
		reply.VoteGranted = false
		// 返回myterm
		reply.Term = rf.currentTerm
		return
	}

	// addsth
	reply.Term = rf.currentTerm

	// if RPC contains term T > currentTerm;
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		DPrintf("[All_Server_Rules_rule_2]Server %d(term %d) into follower, candidate %d(term %d)", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		// set currentTerm = T
		rf.currentTerm = args.Term
		// who and if requires reset
		rf.stateChange(FOLLOWER, false)
		rf.persist()
	}

	// check candidate log up to date
	if rf.upToDateOrNot(args.LastLogIndex, args.LastLogTerm) == false {
		DPrintf("[Violate_RequestVote_RPC_rule_2]Server %d reject %d", rf.me, args.CandidateId)
		reply.VoteGranted = false
		// 返回myterm
		reply.Term = rf.currentTerm
		return
	}

	// 这时候确保了当前rf的currentTerm和requestvote传过来的args.term是一样的
	// 如果rf给别人投过票了， 也是rule 2
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == reply.Term {
		DPrintf("[Violate_RequestVote_RPC_rule_2]Server %d reject %d, Have voter for %d", rf.me, args.CandidateId, rf.votedFor)
		reply.VoteGranted = false
		// 返回myterm
		reply.Term = rf.currentTerm
		return
	} else {
		// 啊 终于可以正常的投票了
		// 真不容易啊
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		// 返回myterm
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.lastElectionTime = time.Now()
		rf.persist()
		DPrintf("[Election_SUCCESS]Server %d voted for %d!!!", rf.me, args.CandidateId)
		return

	}

	return

}

// 获取最近index
func (rf* Raft) lastIdx() int {
	return len(rf.log) - 1
}

// 获取最近term
func (rf* Raft) lastTerm() int {
	if len(rf.log) - 1 == 0 {
		return 0
	} else {
		return rf.log[len(rf.log) - 1].Term
	}
}

// 获取当前index对应的term
func (rf* Raft) myTermOnCertainIndex(Index int) int {
	if Index == 0 {
		return 0
	} else {
		return rf.log[Index].Term
	}
}

// 获取prevlog的信息
func (rf* Raft) prevLogIdxAndTerm(server int) (int, int) {
	prevLogIndex := rf.nextIndex[server] - 1
	// 这个if能不能去掉
	if prevLogIndex == rf.lastIdx() + 1 {
		prevLogIndex = rf.lastIdx()
	}
	return prevLogIndex, rf.myTermOnCertainIndex(prevLogIndex)
}

// 获取当前index的log
func (rf* Raft) myLogOnCertainIndex(Index int) Entry {
	return rf.log[Index]
}

// 看看是不是在term和index上都up to date了
func (rf* Raft) upToDateOrNot(index int, term int) bool {
	// rf是当前节点，index和term是要比较的
	lastIndex := rf.lastIdx()
	lastTerm := rf.lastTerm()
	//若term和index都比rf的要新
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

// 更换raft节点的状态，并初始化一些东西
func (rf *Raft) stateChange(who int, reset bool) {

	// 如果要变成follower
	if who == FOLLOWER {
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.getVoteNum = 0
		// 如果要重置TIMEOUT起始时间
		rf.persist()
		if reset {
			rf.lastElectionTime = time.Now()
		}
	}

	// 如果要变成candidate
	if who == CANDIDATE {
		rf.state = CANDIDATE
		// 自己给自己投票
		rf.votedFor = rf.me
		rf.getVoteNum = 1
		// term更新
		rf.currentTerm += 1
		// 加入选举过程
		rf.persist()
		rf.takePartInElection()
		// 变成candidate一定会reset
		// addsth 先选举后重置时间
		rf.lastElectionTime = time.Now()
	}

	// 如果要变成leader
	if who == LEADER {
		rf.state = LEADER
		rf.votedFor = -1
		rf.getVoteNum = 0
		rf.persist()
		// 后续用到的nextIndex和matchIndex
		// 初始化
		rf.nextIndex = make([]int, len(rf.peers))
		// addsth 忘了初始化了

		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.lastIdx() + 1
		}

		rf.matchIndex = make([]int, len(rf.peers))
		// addsth 自己可以初始化
		rf.matchIndex[rf.me] = rf.lastIdx()
		//重置时间
		rf.lastElectionTime = time.Now()
	}
}

// candidate进入election后的操作
func (rf *Raft) takePartInElection() {
	DPrintf("[Candidate_In_Election]Sever %d, term %d", rf.me, rf.currentTerm)
	// 给其他peers发一个RequestVote
	for index := range rf.peers {
		// 自己不用发
		if index == rf.me {
			continue
		}
		// 其他的peers拉起一个go routine
		go func(server int) {
			// candidate每次发之前先锁定
			rf.mu.Lock()
			// 传入RequestVote的参数
			requestVoteArgs := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				rf.lastIdx(), // 后面再改
				rf.lastTerm(), // 后面再改
			}
			// 定义一个空对象来接受返回值
			requestVoteReply := RequestVoteReply{}
			// 遇到需要等待的rpc就先解锁
			rf.mu.Unlock()
			re := rf.sendRequestVote(server, &requestVoteArgs, &requestVoteReply)
			// 如果requestVoteRpc保证args传的term是大于等于currentTerm，返回true
			if re == true {
				// 先锁再判断状态
				rf.mu.Lock()
				// 如果candidate的state或term在传播rpc的时间中改变了
				if rf.state != CANDIDATE || requestVoteArgs.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				// 若candidate获得了投票并且rpc的term和目前的相等（期待的情况）
				if requestVoteReply.VoteGranted == true && rf.currentTerm == requestVoteArgs.Term {
					rf.getVoteNum += 1
					// 看看是否超过半数
					if rf.getVoteNum >= len(rf.peers) / 2 + 1 {
						DPrintf("[Candidate_To_LEADER_Success] %d got votenum: %d, become leader for term %d", rf.me, rf.getVoteNum, rf.currentTerm)
						rf.stateChange(LEADER, true)
					}
					rf.mu.Unlock()
					return
				}
				// 若candidate收到的rpc回答的Term大于传过去的Term
				if requestVoteReply.Term > requestVoteArgs.Term {
					// 若当前的term还是小了,更新
					if rf.currentTerm < requestVoteReply.Term {
						rf.currentTerm = requestVoteReply.Term
					}
					// 变follower
					rf.stateChange(FOLLOWER, false)
					rf.mu.Unlock()
					return
				}

				//其余情况解锁
				rf.mu.Unlock()
				return
			}
		}(index)
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

// 开始新的log entry的共识
// 所以它得是leader
// start是client给leader发信息
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果宕机了
	if rf.killed() == true {
		return -1, -1, false
	}
	// 如果不是leader就不能作为start的发起人
	if rf.state != LEADER {
		return -1, -1, false
	} else {
		// index 是下一个command出现的位置
		index := rf.lastIdx() + 1
		term := rf.currentTerm
		rf.log = append(rf.log, Entry{Term: term, Command: command})
		rf.persist()
		DPrintf("[Leader_Start] Leader %d get command %v, index %d", rf.me, command, index)
		return index, term, true
	}
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
func (rf *Raft) checkElection() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// 在nowTime开始持续的一段时间内必须要有lastReset
		// 否则f或c将变成c
		nowTime := time.Now()
		duration := getMyElectionTimeOut(int64(rf.me))
		time.Sleep(time.Duration(duration) * time.Millisecond)

		// 加锁判断是否超时
		rf.mu.Lock()
		if rf.lastElectionTime.Before(nowTime) && rf.state != LEADER {
			rf.stateChange(CANDIDATE, true)
		}
		rf.mu.Unlock()

	}
}

// 返回随机的electiontime
func getMyElectionTimeOut(myIndex int64) int {
	//随机种子，加入自身index加强随机
	rand.Seed(time.Now().Unix() + myIndex)
	return rand.Intn(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN
}

// 开始定义appendentreis的输入输出
type AppendEntriesArgs struct {
	// 只是简单地传一下心跳
	Term int
	LeaderId int
	// part b
	PrevLogIndex int
	// prelogindex是加入位置的前一个位置
	PrevLogTerm int
	Entries []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// 只是简单地接受一下心跳
	Term int
	Success bool
	ConflictIndex int // 找nextIndex的优化方式
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()


	// rule 1: 若leader给的term比我自己的还小
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = -1
		return
	}

	//否则， leader的term不比我小
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	reply.Success = true
	reply.ConflictIndex = -1

	// 更改自己的状态
	if rf.state != FOLLOWER {
		rf.stateChange(FOLLOWER, true)
	} else {
		rf.lastElectionTime = time.Now()
		rf.persist()
	}

	// 这里有矛盾
	if args.PrevLogIndex < 0 {
		reply.Success = false
		reply.ConflictIndex = rf.lastIdx() + 1
		return
	}

	// rule2
	// 若rf的最后一个index比prelogIndex小
	// 也就是rf missing了一部分信息
	if rf.lastIdx() < args.PrevLogIndex {
		reply.Success = false
		// 直接返回follower当前最后的index
		// 不需要leader来nextIndex--
		reply.ConflictIndex = rf.lastIdx()
		DPrintf("[AppendEntries_rule_2_args.PrevLogIndex > rf.lastIdx]Sever %d, args.prevLogIndex %d, args.Term %d, rf.lastIdx %d, rf.lastTerm %d", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.lastTerm(), rf.lastTerm())
		return
	} else {
		// 虽然rf的index至少和prevlogindex一样大了
		// 如果rf上prevlogindex对应的term和args的不匹配
		if rf.myTermOnCertainIndex(args.PrevLogIndex) != args.PrevLogIndex{
			// rule3 把不同sameindex不同term的去掉
			reply.Success = false
			tempTerm := rf.myTermOnCertainIndex(args.PrevLogIndex)
			// 把rf中tempTerm的中的log全部视为不一致
			// 然后把所有这个不一致的Term的内容删掉
			for index := args.PrevLogIndex; index >= 0; index-- {
				if rf.myTermOnCertainIndex(index) != tempTerm {
					// ConflictIndex 应该是已经出现矛盾的第一个index
					reply.ConflictIndex = index + 1
					DPrintf("[AppendEntries_rule_3_inconsistency_diff_term]Sever %d, args.prevLogIndex %d, args.Term %d, rf.myTermOnCertainIndex %d", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.myTermOnCertainIndex(args.PrevLogIndex))
					break
				}
			}
			return
		}
	}

	// 至少不missing且在prelogindex上保持term一致
	// rule4: prelogindex前已经一致，从prelogindex后加入
	rf.log = append(rf.log[:args.PrevLogIndex + 1], args.Entries...)
	rf.persist()
	// rule5:leadercommit > commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.changeCommitIndex(FOLLOWER, args.LeaderCommit)
	}
	DPrintf("[HeartBeatFinish]Server %d, from leader %d(term %d), me.lastIndex %d", rf.me, args.LeaderId, args.Term, rf.lastIdx())
	return

}

func (rf *Raft) changeCommitIndex(who int, leaderCommit int) {

	if who != LEADER {
		// rule 5
		if leaderCommit > rf.commitIndex {
			if leaderCommit >= rf.lastIdx() {
				rf.commitIndex = rf.lastIdx()
			} else {
				rf.commitIndex = leaderCommit
			}
		}
		DPrintf("[ChangeCommitIndex] Follower %d commitIndex %d", rf.me, rf.commitIndex)
		return
	}

	if who == LEADER {
		// 初始化
		rf.commitIndex = 0
		// Leaders Rules 4
		// find the max N
		for index := rf.lastIdx(); index >= 1; index-- {
			sum := 0
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					sum += 1
					continue
				}
				// 对于其他follower
				if rf.matchIndex[i] >= index {
					sum += 1
				}
			}
			// 根据Rule 4判断
			if sum >= len(rf.peers) / 2 + 1 && rf.myTermOnCertainIndex(index) == rf.currentTerm {
				rf.commitIndex = index
				break
			}
		}
		DPrintf("[ChangeCommitIndex] Leader %d(term%d) commitIndex %d",rf.me,rf.currentTerm,rf.commitIndex)
		return
	}



}

func (rf* Raft) leaderAppendEntries() {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			// 若leader不是leader了
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}

			// 非法情况返回
			prevLogIndextemp := rf.nextIndex[server] - 1
			if prevLogIndextemp < 0 {
				rf.mu.Unlock()
				return
			}

			//准备输入参数
			aeArgs := AppendEntriesArgs{}

			// 比较leader的lastindex和follower的nextindex
			if rf.lastIdx() >= rf.nextIndex[server] {
				DPrintf("[LeaderAppendEntries]Leader %d (term %d) to server %d, from %d to %d",rf.me,rf.currentTerm,server,rf.nextIndex[server],rf.lastIdx())
				entriesNeeded := make([]Entry, 0)
				// leader 把全部的entries都塞进去
				entriesNeeded = append(entriesNeeded, rf.log[rf.nextIndex[server]:]...)
				prevLogIndex, preLogTerm := rf.prevLogIdxAndTerm(server)
				aeArgs = AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					prevLogIndex,
					preLogTerm,
					entriesNeeded,
					rf.commitIndex,
				}
			} else {
				// follower的nextindex居然比本leader的还要大！过分！
				prevLogIndex, prevLogTerm := rf.prevLogIdxAndTerm(server)
				aeArgs = AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					prevLogIndex,
					prevLogTerm,
					[] Entry{},
					rf.commitIndex,
				}
				DPrintf("[Sever_has_big_nextIndex] Leader %d (term %d, LastIndex %d) to server %d, nextIndex %d", rf.me, rf.currentTerm, rf.lastIdx(), server, rf.nextIndex[server])
			}


			DPrintf("[SendHeartBeat]Leader %d (term %d) to server %d", rf.me, rf.currentTerm, server)

			//准备输出参数
			appendEntriesReply := AppendEntriesReply{}
			//rpc之前先释放锁！！！
			rf.mu.Unlock()

			//注意rpc的传输要用&，因为通过地址传递可以获得改变后的reply
			re := rf.sendAppendEntries(server, &aeArgs, &appendEntriesReply)

			if re == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// 若leader不是leader
				if rf.state != LEADER {
					return
				}

				// 若follower返回的term更大！
				if appendEntriesReply.Term > rf.currentTerm {
					//改term降级
					// All Servers rule 2
					rf.currentTerm = appendEntriesReply.Term
					rf.stateChange(FOLLOWER, true)
					return
				}


				// append成功了
				if appendEntriesReply.Success {
					DPrintf("[Append_success] Leader %d (term %d), from Server %d, prevLogIndex %d", rf.me, rf.currentTerm, server, aeArgs.PrevLogIndex)
					// 更新leader中该server的match（已匹配）和next（下一个位置）
					rf.matchIndex[server] = aeArgs.PrevLogIndex + len(aeArgs.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					// leader检查是否要commit
					rf.changeCommitIndex(LEADER, 0)
				}

				// append失败了
				if !appendEntriesReply.Success {
					// 若找到矛盾的位置
					if appendEntriesReply.ConflictIndex != -1 {
						DPrintf("[HeartBeat WRONG] Leader %d (term %d) ,from Server %d, prevLogIndex %d, Confilicting %d",rf.me,rf.currentTerm,server,aeArgs.PrevLogIndex,appendEntriesReply.ConflictIndex)
						// 有效率地更改nextIndex
						rf.nextIndex[server] = appendEntriesReply.ConflictIndex
					}
				}


			}


		}(index)
	}
}

func (rf *Raft) checkAppendEntries() {
	for rf.killed() == false {
		// leader间隔心跳时间发appendentries
		time.Sleep(HEARTBEAT_TIMEOUT * time.Millisecond)
		rf.mu.Lock()
		if rf.state == LEADER {
			// rpc之前先释放锁，否则要等整个rpc！！！
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}

	}
}

func (rf* Raft) checkApplied() {
	// 每个rf将committed的内容apply
	for rf.killed() == false{
		time.Sleep(APPLY_TIMEOUT * time.Millisecond)
		rf.mu.Lock()

		// 都提交完了
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		// 每个raft都要把提交的messages发给chan
		Messages := make([]ApplyMsg, 0)
		// 还可以提交且在index内
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.lastIdx() {
			// 充实Messages
			rf.lastApplied += 1
			Messages = append(Messages, ApplyMsg{
				CommandValid: true,
				SnapshotValid: false,
				CommandIndex: rf.lastApplied,
				Command: rf.myLogOnCertainIndex(rf.lastApplied).Command,
			})
		}
		rf.mu.Unlock()

		for _, messages := range Messages {
			rf.applyCh <- messages
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
	// peers对应的index就是每个server唯一的id
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 还是要加锁
	rf.mu.Lock()
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.getVoteNum = 0
	rf.votedFor = -1
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.log = []Entry{}
	rf.log = append(rf.log, Entry{}) // 最开始有一个空的便于处理
	rf.applyCh = applyCh
	rf.mu.Unlock()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	// start ticker goroutine to start elections
	// go rf.ticker()

    // 定时选举candidate
	go rf.checkElection()

	// leader定时发心跳，和log entries
	go rf.checkAppendEntries()

	// 定时将committed变成applied
	// 并将apply的内容写进rf.applyCh中
	go rf.checkApplied()



	return rf

}
