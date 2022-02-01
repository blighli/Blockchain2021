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
	rand2 "math/rand"
	"sort"

	//	"bytes"
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

// 单个的日志项
type LogEntry struct {
	Term 		int
	Command 	interface{}
	Index 		int
}

type State string
const (
	Follower State = "follower"
	Candidate State = "candidate"
	Leader State = "leader"
	ELECTION_INTERVAL int = 300
	TIMEOUT int = 300
	HEART_BEAT int = 100
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	co        sync.Mutex
	mu1		  sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm 		int //当前的server见过的最新的term
	voteFor 			int //candidateId that received vote in current term
	state 				State //当前的server的状态
	lastReceived 		time.Time //最后一次收到其他服务器消息的时间

	//(2B)
	logs				[]LogEntry //本地的log的日志数组
	commitIndex 		int         // 已经提交的最高的日志索引
	lastApplied 		int         // 被应用到状态机的最大的日志索引
	nextIndex 			[]int     // 对于每一个server，leader要发送给他的下一个日志索引
	matchIndex 			[]int    // 对于每一个server，leader知道的已经apply到状态机的最大的日志索引
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
	isleader = rf.state == Leader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	DPrintf("[%d] persist state",rf.me)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	//Your code here (2C).
	//Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.logs)
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
// 转换为Candidate时向其他的节点发送请求投票投自己
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int //候选者自己的term
	CandidateId int //候选者自己的id
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
// 投票返回回来的信息
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int  //投票者自己的term
	VoteGranted bool  //投票者是否投了自己一票
}

// 用于响应leader的追加日志，同时也可以作为心跳信息使用
type AppendEntriesArgs struct {
	Term int //leader的term
	LeaderId int //leader的id
	PrevLogIndex int
	PrevLogTerm int
	LeaderCommit int // leader's commitIndex
	Entries	[]LogEntry
}

type AppendEntriesReply struct {
	Term int //返回的term，用于leader对比更新自己的term
	Success bool //
	ConflictIndex int
	ConflictTerm int
	FollowerLength int
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d]:receive vote [Trem:%d , peer:%d] request from [%d]",rf.me,args.Term,args.CandidateId,
		args.CandidateId)
	if args.Term > rf.currentTerm {//如果Candidate的term比当前的term大的话，当前转换为Follower，但是不一定就投票了
		rf.convertToFollower(args.Term)
		// 还是要判断日志的新旧问题
		// 条件：1.Candidate的日志的最大term比我的最大term大 2.Candidate的最大term与我的相等，但是最大的index不小于我的最大index
		if len(rf.logs) == 0 || args.LastLogTerm > rf.logs[len(rf.logs)-1].Term || (args.LastLogIndex >= len(rf.logs) &&
			args.LastLogTerm == rf.logs[len(rf.logs)-1].Term){
			reply.VoteGranted = true
			rf.lastReceived = time.Now()
			rf.voteFor = args.CandidateId
			DPrintf("[%d]:give vote support to [%d]",rf.me,args.CandidateId)
			rf.persist()
		}else{
			reply.VoteGranted = false
			DPrintf("[%d] don't vote to [%d],cause [index:%d,term:%d] less than [index:%d,term:%d]",
				rf.me,args.CandidateId,args.LastLogIndex,args.LastLogTerm,len(rf.logs),rf.logs[len(rf.logs)-1].Term)
		}
	}else if args.Term < rf.currentTerm {//如果Candidate的term比当前的term还小，直接返回false
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("[%d]:the term of [%d] is less than mine",rf.me,args.CandidateId)
	}else {
		// term相同的时候，几个条件：
		// 1.当前还没投票或者投的就是这个 (充分条件) 2.Candidate的最大的term大于我的最大的term 3.term相等，但是最后一个index
		// 不小于我的最大的index
		if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && (len(rf.logs) == 0 || args.LastLogTerm > rf.logs[len(rf.logs)-1].Term ||
			(args.LastLogIndex >= len(rf.logs) && args.LastLogTerm == rf.logs[len(rf.logs)-1].Term)){
			rf.lastReceived = time.Now()
			DPrintf("[%d]:give vote support to [%d] when term is equal.",rf.me,args.CandidateId)
			rf.voteFor = args.CandidateId
			reply.VoteGranted = true
			rf.persist()
		}else{
			reply.VoteGranted = false
			//DPrintf("[%d] dont't vote to [%d],cause [voteFor:%d,lastIndex:%d,lastTerm:%d ====>  CandidateId:%d,i:%d,t:%d]",
			//	rf.me,args.CandidateId,rf.voteFor,len(rf.logs),rf.logs[len(rf.logs)-1].Term,args.CandidateId,args.LastLogIndex,
			//	args.Term)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("[%d]:receive heartBeat from [%d]",rf.me,args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		rf.convertToFollower(args.Term) // 先转换为Follower
		rf.lastReceived = time.Now()
		reply.Term = rf.currentTerm
		//if len(args.Entries)!=0 {//说明发送的是单纯的心跳信息，没有追加日志的操作
			// 不能直接添加的条件：
			// 1.prevIndex 是0，说明大家都没有日志，直接加
			// 1.previndex 比我的最大的index还要大，说明前面的日志一定不是同步的
			// 2.如果prev在我的日志的索引范围内，比较对应的两个日志的索引的term，不相等说明一定错
			// 3.如果prev是0 是相等的，因为初始化的时候，0号索引的term都是-1 也能过这个条件，所以能添加1号索引的日志项
			if args.PrevLogIndex > len(rf.logs){
				//(len(rf.logs)>0 && args.Entries[len(args.Entries)-1].Term < rf.logs[len(rf.logs)-1].Term){ //todo mark 加一行双重验证，防止特定的条件错误
				reply.Success = false
				DPrintf("[%d,pI:%d,pT:%d] failed to append entries to [%d,I:%d]",args.LeaderId,args.PrevLogIndex,
					args.PrevLogTerm,rf.me,len(rf.logs))
				reply.FollowerLength = len(rf.logs)
				reply.ConflictIndex = -1
				reply.ConflictTerm = -1
				return
			}
			if args.PrevLogIndex!=0 && rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm {
				reply.Success = false
				DPrintf("[%d,pI:%d,pT:%d] failed to append entries to [%d,I:%d]",args.LeaderId,args.PrevLogIndex,
					args.PrevLogTerm,rf.me,len(rf.logs))
				term := rf.logs[args.PrevLogIndex-1].Term
				tempIndex := -1
				for i := args.PrevLogIndex - 1; i >= 0; i-- {
					if rf.logs[i].Term != term {
						tempIndex = i + 1
						break
					}
				}
				reply.FollowerLength = -1
				reply.ConflictTerm = term
				reply.ConflictIndex = tempIndex  // 如果tempindex为-1，那么下一个插入的位置就是1
				return
			}
			if len(args.Entries)!=0 {
				rf.logs = rf.logs[:args.PrevLogIndex] //截取当前的已经同步的日志
				DPrintf("ready to append logs[len = %d].", len(args.Entries))
				for i := range args.Entries { //插入append的日志项
					rf.logs = append(rf.logs, args.Entries[i])
				}
				DPrintf("[%d] append logs to [%d] successfully and lastIndex of logs is [%d]", args.LeaderId, rf.me, len(rf.logs))
				rf.printAll()
				rf.persist()
			}else {
				DPrintf("[%d] to [%d] empty heartbeat",args.LeaderId,rf.me)
			}
			//if len(rf.logs) > 0 && rf.logs[args.LeaderCommit - 1].Term == rf.currentTerm{
				rf.commitIndex = min(args.LeaderCommit, len(rf.logs)) // 更新commitIndex的时候要加上只更新当前term的条件
				DPrintf("[%d] commitIndex is [%d]",rf.me,rf.commitIndex)
				go rf.apply()
			//}
		reply.Success = true
	}else { //如果term小于的话，直接返回false
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("[%d] try to appendEntries to [%d] failed,cause [%d,t:%d] is less than [%d,t:%d]",
			args.LeaderId,rf.me,args.LeaderId,args.Term,rf.me,rf.currentTerm)
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
	ok := rf.peers[server].Call("Raft.AppendEntries",args,reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B)
	DPrintf("Client call Start of peer[%d]",rf.me)
	if rf.state != Leader {
		isLeader = false
	}else {
		rf.logs = append(rf.logs, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
			Index:   len(rf.logs) + 1, // log的长度为1，则插入的索引应该为1，索引从1开始计数
		})
		rf.persist()
		index = len(rf.logs)// 插入的索引的位置，因为当前还没真实插入，所以就是len(logs)
		term = rf.currentTerm
		isLeader = true
		rf.nextIndex[rf.me] = len(rf.logs) + 1
		rf.matchIndex[rf.me] = len(rf.logs)
		rf.printAll()
	}
	DPrintf("[%d] raft start return to client [%d,%d,%t]",rf.me,index,term,isLeader)
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
// 用于开启一轮新的选举当心跳超时的时候
func (rf *Raft) ticker() {
	DPrintf("[%d]:ticker",rf.me)
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep();
		startTime := time.Now()
		time.Sleep(time.Duration(ELECTION_INTERVAL + rand2.Intn(TIMEOUT))*time.Millisecond)
		//DPrintf("[%d] start !",rf.me)
		if rf.lastReceived.Before(startTime) {
			if rf.state != Leader {
				DPrintf("[%d,state:%s,term:%d] is election timeout,and to election",rf.me,rf.state,rf.currentTerm)
				rf.AttemptLeaderElection()
			}
		}
	}
}
//todo 在机器被隔离不断地重新选举的时候，对应的term要不要加一，造成的问题就是如果网络变通畅了，
// 他的term比其他的term要大但是却没有最新的日志信息。这样就会造成集群选举不出来新的leader，因为它永远最先超时，然后给其他机器发投票请求
// 它给其他机器发投票请求的时候，如果不投给他我就不重置选举超时时间？
func (rf *Raft) AttemptLeaderElection() {
	rf.convertToCandiate()
	lastLogTerm := -1
	if len(rf.logs) > 0 {
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	}
	args:= RequestVoteArgs{//不需要加锁，因为只有当前的选举线程会调用
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: len(rf.logs), // 最后一个log的索引值，如果logs长度为1，最后一个索引为0
		LastLogTerm: lastLogTerm,// 最后一个索引值的term
	}
	cond:=sync.NewCond(&rf.co)
	voteCount := 1 //自己投自己一票
	totalCount := 1
	for i:= range rf.peers {
		go func(server int) {//投票
			if server != rf.me {//不会给自己发送投票信息
				// 为什么把发送心跳房子锁的外面呢？
				// 多线程之所以多线程就是为了异步的进行请求，加锁不就是南辕北辙了吗？
				// todo 记录发生了共享变量的修改错误问题 多线程可能会改变他的值
				reply:=RequestVoteReply{
					Term: -1,
					VoteGranted: false,
				}
				vote := rf.sendRequestVote(server, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				totalCount +=1
				defer cond.Broadcast()
				if vote==false{
					DPrintf("[%d] requestVoteRPC can't connect to [%d] is %t",rf.me,server,vote)
					return
				}
				DPrintf("[%d] receive vote reply{%t} from [%d]",rf.me,reply.VoteGranted,server)
				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
					rf.lastReceived = time.Now()
				}else if reply.VoteGranted {
					voteCount += 1
					DPrintf("[%d]'s voteCount is %d",rf.me,voteCount)
				}else{
					DPrintf("[%d] vote false cause log is old",rf.me)
				}
			}
		}(i)
	}
	//计票
	go func() {
		//如果选票超过半数或者当前节点不是Candidate或者选票已经发送完了
		DPrintf("=============[%d] start count vote ============",rf.me)
		rf.co.Lock()
		defer rf.co.Unlock()
		tempTerm := rf.currentTerm
		for voteCount <= len(rf.peers)/2 && rf.state == Candidate && totalCount<len(rf.peers) && tempTerm == rf.currentTerm{
			DPrintf("[%d] count rountin is waiting.",rf.me)
			cond.Wait()
		}
		DPrintf("[%d] count rountinis singnal.",rf.me)
		DPrintf("[%d] 's votecount is %d and the state is [%s]",rf.me,voteCount,rf.state)
		if voteCount > len(rf.peers)/2 && rf.state == Candidate && tempTerm == rf.currentTerm{
			rf.convertToLeader()
			DPrintf("[%d,term:%d] become ****leader****",rf.me,rf.currentTerm)
			//time.Sleep(time.Duration(30)*time.Millisecond)//todo 乱写的 不确定要不要睡一会
			go rf.sendHeartBeat()
		}else{
			DPrintf("[%d,Term:%d,state:%s] become leader failed.",rf.me,rf.currentTerm,rf.state)
		}
	}()
}

func max(a int,b int) int{
	if a > b{
		return a
	}else{
		return b
	}
}

func min(a int,b int) int{
	if a >= b{
		return b
	}else{
		return a
	}
}

//发送心跳机制
func (rf *Raft) sendHeartBeat() {
	//cond := sync.NewCond(&rf.co)
	for rf.killed() == false && rf.state == Leader{
		for i := range rf.peers {
			currentTerm := rf.currentTerm
			commitIndex := rf.commitIndex
			if i!=rf.me && rf.state == Leader{ //不给自己发送心跳，同时自己的状态要为leader才行
				go func(server int) {
					//todo 只有更改共享变量的时候才需要加锁，前面都是线程的内部变量，如果加锁，会导致线程的阻塞，
					//导致上下逻辑执行不连贯，造成这个过程中其他线程把日志修改了，也就是说我现在要发的日志已经不是之前那段逻辑
					//的日志了，所以说去掉锁就好了
					cacheNextIndex := rf.nextIndex[server]
					prevLogIndex := cacheNextIndex - 1 // 为了比较要添加的位置的上一个位置是否相等，相等才可以继续进行添加
					entries := make([]LogEntry,0)
					// 如果nextIndex的值比当前的logs的最大索引值小的话，否则的话就是没有要传的logs，就是空心跳信息
					if cacheNextIndex <= len(rf.logs){
						entries = rf.logs[cacheNextIndex-1:] //截取nextindex后的logs
						DPrintf("[%d] lastIndex of logs is [%d],nextIndex of [%d] is [%d],length of entries to send is [%d]",
							rf.me,len(rf.logs), server,cacheNextIndex,len(entries))
					}
					DPrintf("[%d]:lastLogIndex is [%d],nextIndex of [%d] is %d,length of entries is [%d]",
						rf.me,len(rf.logs),server,cacheNextIndex,len(entries))
					prevLogTerm := -1
					if prevLogIndex > len(rf.logs) {
						prevLogIndex = len(rf.logs)
						prevLogTerm = rf.logs[prevLogIndex-1].Term
					} else if prevLogIndex >0 {
						prevLogTerm = rf.logs[prevLogIndex-1].Term
					}
					args:=AppendEntriesArgs{
						Term: currentTerm,
						LeaderId: rf.me,
						LeaderCommit: commitIndex,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm: prevLogTerm,
						Entries: entries,
					}
					reply:=AppendEntriesReply{ConflictIndex: -1,ConflictTerm: -1,FollowerLength: -1}
					//rf.mu.Unlock()
					ok := rf.sendAppendEntries(server, &args, &reply)
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if ok {
						if reply.Success == false {
							if reply.Term > rf.currentTerm{
								rf.convertToFollower(reply.Term)
								rf.lastReceived = time.Now()
								return
							}
							// todo nextIndex 优化
							if reply.FollowerLength != -1 {
								rf.nextIndex[server] = max(1,reply.FollowerLength + 1)
							}else {
								tempIndex := -1
								for i := args.PrevLogIndex; i >0 ; i-- { // nextindex 是不能变大的，只能回退
									if rf.logs[i-1].Term == reply.ConflictTerm {
										tempIndex = i
										break
									}
								}
								if tempIndex != -1 {
									rf.nextIndex[server] = max(1,tempIndex + 1)
								}else{
									rf.nextIndex[server] = max(1,reply.ConflictIndex + 1)
								}
							}
							//rf.nextIndex[server] = max(1,cacheNextIndex - 1)
							DPrintf("[%d] update nextIndex of [%d] to [%d]",rf.me,server,rf.nextIndex[server])
						}else {// 添加日志成功，说明当前的logs已经全部同步到对应的server中去了
							//如果上次发的空心跳返回之前，客户端添加了一个日志，那么这个结果就会出现错误
							// entries并不是一个共享变量，只会被当前线程调用
							rf.nextIndex[server] = cacheNextIndex + len(entries) // nextIndex自然就是当前的下一个要插入的索引位置了
							DPrintf("[%d] update nextIndex[%d] to %d",rf.me,server,cacheNextIndex)
							rf.matchIndex[server] = cacheNextIndex - 1
							DPrintf("[%d] update matchIndex[%d] to [%d]",rf.me,server,rf.matchIndex[server])
							DPrintf("[peer:%d,term:%d] appendEntries to [peer:%d,term:%d] successfully!", rf.me,
								rf.currentTerm, server, reply.Term)
						}
					}else {
						DPrintf("[%d] appendEntries connect to [%d] failed.",rf.me,server)
					}
				}(i)
			}
		}
		// 计算commitIndex
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			//for i := 0; i < len(rf.peers)-1; i++ {
			//	cond.Wait()
			//	DPrintf("[%d] heartbeat is waiting.",rf.me)
			//}
			//DPrintf("[%d] heartBeat is singnal",rf.me)
			tempIndex := make([]int, len(rf.matchIndex))
			copy(tempIndex, rf.matchIndex)
			sort.Ints(tempIndex)
			DPrintf("[%d] matchIndex sorted is [%d,%d,%d]",rf.me,tempIndex[0],tempIndex[1],tempIndex[2])
			//todo mark 安全性保证 只能提交当前任期内的日志项
			if tempIndex[len(tempIndex)/2] > len(rf.logs) && rf.logs[len(rf.logs)-1].Term == rf.currentTerm{
				rf.commitIndex = len(rf.logs)
				DPrintf("[%d] most of peer's commitIndex is [%d] and length of logs is [%d]",
					rf.me,tempIndex[len(tempIndex)/2-1],len(rf.logs))
			}else if tempIndex[len(tempIndex)/2] <= len(rf.logs) && tempIndex[len(tempIndex)/2] > rf.commitIndex &&
				rf.logs[tempIndex[len(tempIndex)/2]-1].Term == rf.currentTerm {
				rf.commitIndex = tempIndex[len(tempIndex)/2] // 更新成大多数的commitIndex
				DPrintf("[%d] update commitIndex to [%d]", rf.me, rf.commitIndex)
			}
			go rf.apply()
		}()
		time.Sleep(time.Duration(HEART_BEAT)*time.Millisecond)
	}
	DPrintf("[%d,term:%d] leader is killed",rf.me,rf.currentTerm)
}

func (rf *Raft) apply(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		applyMsg := ApplyMsg{
			Command: rf.logs[i-1].Command,
			CommandValid: true,
			CommandIndex: i,
		}
		rf.applyCh <- applyMsg
		DPrintf("[%d] addend log to applyCh,lastApplied = [%d],content = [%v]",rf.me,
			i,applyMsg)
	}
	if rf.lastApplied < rf.commitIndex {
		rf.lastApplied = rf.commitIndex
		DPrintf("[%d] update lastApplied to %d",rf.me,rf.lastApplied)
	}
}

func (rf *Raft) printAll(){
	if len(rf.logs)>0 {
		DPrintf("================PRINT ALL LOGS OF [%d]===================",rf.me)
		for i := range rf.logs {
			DPrintf("[%v]",rf.logs[i])
		}
	}
}

func (rf *Raft) convertToFollower(term int) {
	DPrintf("[%d] begin to convert to follower",rf.me)
	rf.state = Follower
	rf.voteFor = -1
	rf.currentTerm = term
	DPrintf("[%d] convert to follower , the term is [%d]",rf.me,term)
	rf.persist()
}

func (rf *Raft) convertToCandiate() {
	rf.state = Candidate
	rf.voteFor = rf.me
	rf.currentTerm = rf.currentTerm + 1
	rf.lastReceived = time.Now()
	DPrintf("[%d] convert to candidate,the term is [%d]",rf.me,rf.currentTerm)
	rf.persist()
}

func (rf *Raft) convertToLeader(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Leader
	rf.lastReceived = time.Now()
	DPrintf("[%d] convert to Leader",rf.me)
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

// peers包含了全部的raft server的port;me是当前的server的位置;所有的服务器的
// peers是具有相同的顺序的。persister是当前的server的要持久化数据的位置，
// Make必须立即返回，所以它需要为耗时的工作单独开别的线程才行。
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.lastReceived = time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	rf.readPersist(persister.ReadRaftState())
	rf.nextIndex = make([]int,len(rf.peers))
	for i := range rf.nextIndex { // 全部初始化为1
		//todo 纯属没事找事
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int,len(rf.peers))
	// initialize from state persisted before a crash

	DPrintf("make peer instance [id:%d,term:%d,state:%s]",rf.me,rf.currentTerm,rf.state)
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}