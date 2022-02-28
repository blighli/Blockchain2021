package raft

import (
	"log"
	"math"
	"math/rand"
)

type AppendEntriesArgs struct {
	Flag         int32       // 随机数，用于标识当前请求
	Term         int32       // leader的term号
	LeaderId     int32       // leader ID
	PrevLogIndex int32       // 新条目的前一个条目的索引
	PrevLogTerm  int32       // 新条目的前一个条目的term号
	LeaderCommit int32       // leader的lastCommittedIndex
	Entries      []*LogEntry // 新的日志条目（为空时，表示当前请求为心跳提醒）
}

type AppendEntriesReply struct {
	Term    int32 // 接收者的term号
	Success bool  // 一致性检查：当接收者存在与PrevLogIndex和PrevLogTerm匹配的日志条目时返回true
	// 用于快速更新nextIndex：
	//	PrevLogIndex超过接收者的日志长度时，记录接收者日志最后一个索引+1
	//	PrevLogIndex的term冲突时，记录冲突term内的第一个index
	ExpectNextIndex int32
}

// AppendEntries 处理函数
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("[%d][term %d][%s] AppendEntries from [%d] <%d> args ==> %+v", rf.me, rf.curTerm, rf.state, args.LeaderId, args.Flag, args)
	// Note: defer 传入的是当前的变量值，并非函数结束的变量值。若想看到函数结束时变量的值，需使用指针
	defer DPrintf("[%d][term %d][%s] AppendEntries from [%d] <%d> reply <== %+v", rf.me, rf.curTerm, rf.state, args.LeaderId, args.Flag, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.curTerm {
		// 对方 term 过期直接拒绝
		reply.Term = rf.curTerm
		reply.Success = false
		return
	} else if args.Term > rf.curTerm {
		// 若自己 term 过期，立即更新成那个较大的值，变为 follower 状态
		rf.curTerm = args.Term
		rf.becomeFollower()
		rf.persist()
	} else {
		// 任期相同
		if rf.state == leaderState {
			// 如果自己也是leader，报错
			log.Fatalf("[%d][term %d][%s] 一个term内出现两个leader", rf.me, rf.curTerm, rf.state)
		} else if rf.state == candidateState {
			// 如果自己是 candidate 状态，则变为 follower 状态
			rf.becomeFollower()
			rf.persist()
		}
	}
	rf.discoverLeader = true // 标记发现合法leader
	reply.Term = rf.curTerm

	// 一致性检查（优化：跳过冲突的term，快速回退nextIndex）
	if args.PrevLogIndex == 0 {
		reply.Success = true
	} else if args.PrevLogIndex > rf.getLastLogIndex() {
		// PrevLogIndex 超过日志长度
		reply.Success = false
		reply.ExpectNextIndex = rf.getLastLogIndex() + 1
		return
	} else if args.PrevLogIndex < rf.lastIncludedIndex {
		// 上一个条目索引小于快照最后一个索引
		reply.Success = false
		reply.ExpectNextIndex = rf.lastIncludedIndex + 1
		return
	} else if args.PrevLogIndex == rf.lastIncludedIndex {
		// 上一个条目索引等于快照的最后索引，则term一定相等（快照是已提交的），若不相等报错
		if args.PrevLogTerm != rf.lastIncludedTerm {
			log.Fatalf("[%d][term %d][%s] args.PrevLogTerm(%d) != rf.lastIncludedTerm(%d)", rf.me, rf.curTerm, rf.state, args.PrevLogTerm, rf.lastIncludedTerm)
		}
		reply.Success = true
	} else if rf.logs[rf.getActLogIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		// PrevLogIndex索引处条目term不相等
		reply.Success = false
		conflictTerm := rf.logs[rf.getActLogIndex(args.PrevLogIndex)].Term
		p := args.PrevLogIndex
		for p > 0 && rf.logs[rf.getActLogIndex(p)].Term == conflictTerm {
			p--
			if p == rf.lastIncludedIndex && rf.lastIncludedTerm == conflictTerm {
				log.Fatalf("[%d][term %d][%s] 回退到快照最后一个索引(%d)，依旧在冲突term(%d)内", rf.me, rf.curTerm, rf.state, p, conflictTerm)
			}
		}
		reply.ExpectNextIndex = p + 1
		return
	} else {
		reply.Success = true
	}

	// 若不新日志条目不为空，更新自己的日志
	if len(args.Entries) > 0 {
		// 删除PrevLogIndex位置之后的剩余条目，追加新条目
		rf.logs = rf.logs[:rf.getActLogIndex(args.PrevLogIndex+1)]
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()
		DPrintf("[%d][term %d][%s] append new LogEntries: %+v", rf.me, rf.curTerm, rf.state, rf.logs)
	}

	// 更新已提交记录索引值
	if rf.lastCommittedIndex < args.LeaderCommit {
		// set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit < rf.getLastLogIndex() {
			rf.lastCommittedIndex = args.LeaderCommit
		} else {
			rf.lastCommittedIndex = rf.getLastLogIndex()
		}
	}

	// 执行已提交记录中的命令
	applyMsgs := make([]ApplyMsg, 0)
	for rf.lastAppliedIndex < rf.lastCommittedIndex {
		rf.lastAppliedIndex++
		applyMsgs = append(applyMsgs, ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.getActLogIndex(rf.lastAppliedIndex)].Command,
			CommandIndex: int(rf.lastAppliedIndex),
		})
	}
	go rf.applyLogEntries2service(applyMsgs)

	return
}

func (rf *Raft) sendAppendEntries(server int,
	args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	args.Flag = rand.Int31n(math.MaxInt32)
	DPrintf("[%d][term %d][%s] AppendEntries to [%d] <%d> args ==> %+v", rf.me, rf.curTerm, rf.state, server, args.Flag, args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("[%d][term %d][%s] AppendEntries to [%d] <%d> reply %v <== %+v", rf.me, rf.curTerm, rf.state, server, args.Flag, ok, reply)
	return ok
}
