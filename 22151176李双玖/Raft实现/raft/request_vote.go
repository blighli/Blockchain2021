package raft

import (
	"math"
	"math/rand"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Flag         int32 // 随机数，用于标识当前请求
	Term         int32 // 候选人的term号
	CandidateId  int32 // 候选人ID
	LastLogIndex int32 // 候选人最后一个日志条目的索引
	LastLogTerm  int32 // 候选人最后一个日志条目的term号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32 // 投票者的term号
	VoteGranted bool  // 是否投票
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("[%d][term %d][%s] RequestVote from [%d] <%d> args ==> %+v", rf.me, rf.curTerm, rf.state, args.CandidateId, args.Flag, args)
	defer DPrintf("[%d][term %d][%s] RequestVote from [%d] <%d> reply <== %+v", rf.me, rf.curTerm, rf.state, args.CandidateId, args.Flag, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 对方 term 过期直接拒绝请求
	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm
		reply.VoteGranted = false
		return
	}

	// 发现自己 term 过期时，立即更新成那个较大的值，变为 follower 状态
	if args.Term > rf.curTerm {
		rf.curTerm = args.Term
		rf.becomeFollower()
		rf.persist()
	}
	reply.Term = rf.curTerm

	// 同意投票：状态为follower，且未投票给其它人，且候选人的日志跟自己一样新或更新
	// 通过比较两个日志中最后一条记录来判断谁更新：term序号大的更新；term序号相等但日志更长的更新。
	if rf.state == followerState && rf.votedFor == -1 &&
		(args.LastLogTerm > rf.getLastLogTerm() ||
			(args.LastLogTerm == rf.getLastLogTerm() &&
				args.LastLogIndex >= rf.getLastLogIndex())) {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		return
	}

	reply.VoteGranted = false
	return
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
func (rf *Raft) sendRequestVote(server int,
	args *RequestVoteArgs, reply *RequestVoteReply) bool {
	args.Flag = rand.Int31n(math.MaxInt32)
	DPrintf("[%d][term %d][%s] RequestVote to [%d] <%d> args ==> %+v", rf.me, rf.curTerm, rf.state, server, args.Flag, args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("[%d][term %d][%s] RequestVote to [%d] <%d> reply %v <== %+v", rf.me, rf.curTerm, rf.state, server, args.Flag, ok, reply)
	return ok
}
