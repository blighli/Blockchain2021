package raft

import (
	"sync/atomic"
	"time"
)

func (rf *Raft) becomeCandidate() {
	DPrintf("[%d][term %d][%s] becomeCandidate", rf.me, rf.curTerm, rf.state)
	rf.mu.Lock()
	rf.curTerm++
	rf.state = candidateState
	rf.votedFor = int32(rf.me)
	rf.persist()
	rf.mu.Unlock()
	rf.requestVotes2servers(rf.curTerm)
}

// requestVotes2servers 向其它服务器请求投票
func (rf *Raft) requestVotes2servers(term int32) {
	DPrintf("[%d][term %d][%s] requestVotes2servers", rf.me, rf.curTerm, rf.state)
	voteCnt := int32(1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.requestVotes2server(term, i, &voteCnt)
	}
}

// requestVotes2server 向服务器请求投票
func (rf *Raft) requestVotes2server(term int32, index int, voteCnt *int32) {
	var reply RequestVoteReply
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  int32(rf.me),
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	// 请求不成功，则一直重试
	for rf.killed() == false && rf.state == candidateState && term == rf.curTerm &&
		!rf.sendRequestVote(index, &args, &reply) {
		time.Sleep(10 * time.Millisecond)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 等待请求过久，状态可能发生变更
	if rf.killed() != false || rf.state != candidateState || term != rf.curTerm {
		return
	}
	if reply.Term > rf.curTerm {
		// 对方term较大，标记选举结束，更新term值，更新状态为follower
		rf.curTerm = reply.Term
		rf.becomeFollower()
		rf.persist()
	} else if reply.VoteGranted {
		atomic.AddInt32(voteCnt, 1)
		// 得到的票数若超过半数则成为leader
		if atomic.LoadInt32(voteCnt) > int32(len(rf.peers)/2) {
			go rf.becomeLeader()
		}
	}
}
