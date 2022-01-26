package raft

import (
	"time"
)

func (rf *Raft) becomeLeader() {
	DPrintf("[%d][term %d][%s] becomeLeader", rf.me, rf.curTerm, rf.state)
	rf.mu.Lock()
	if rf.state == leaderState {
		rf.mu.Unlock()
		return
	}
	rf.state = leaderState
	rf.votedFor = -1
	rf.persist()
	term := rf.curTerm // 记录当前 term ，当 rf.curTerm != term 时，说明状态改变
	rf.mu.Unlock()

	// 初始化
	rf.nextIndex = make([]int32, len(rf.peers))
	rf.matchIndex = make([]int32, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}

	// 定期发送AppendEntries请求给其它节点（心跳提醒）
	for rf.killed() == false && rf.curTerm == term && rf.state == leaderState {
		rf.appendEntries2severs(term)
		time.Sleep(200 * time.Millisecond)
	}
}

// appendEntries2sever 向其它服务器发送 AppendEntries 请求。
func (rf *Raft) appendEntries2severs(term int32) {
	DPrintf("[%d][term %d][%s] appendEntries2severs", rf.me, rf.curTerm, rf.state)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// 传入 term 和 rf.nextIndex[i] 用于判断状态是否改变
		go rf.appendEntries2sever(term, i, rf.nextIndex[i])
	}
}

// appendEntries2sever 向服务器<index>发送 AppendEntries 请求。
//	当 Entries 为空时，相当于心跳提醒
func (rf *Raft) appendEntries2sever(term int32, index int, nextIndex int32) {
	var reply AppendEntriesReply
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     int32(rf.me),
		LeaderCommit: rf.lastCommittedIndex,
	}
	p := nextIndex
	for rf.killed() == false && rf.state == leaderState &&
		term == rf.curTerm && rf.nextIndex[index] == nextIndex {
		// p 最小是 1 ，不会等于 0
		if p <= rf.lastIncludedIndex {
			// 如果对方的 nextIndex 小于等于快照的 lastIncludedIndex ，则先发送快照给对方
			go rf.installSnapshot2sever(term, index, rf.nextIndex[index],
				rf.lastIncludedIndex, rf.lastIncludedTerm, rf.snapshotData)
			break
		}

		args.PrevLogIndex = p - 1
		if args.PrevLogIndex == 0 {
			args.PrevLogTerm = -1
		} else if args.PrevLogIndex == rf.lastIncludedIndex {
			args.PrevLogTerm = rf.lastIncludedTerm
		} else {
			args.PrevLogTerm = rf.logs[rf.getActLogIndex(args.PrevLogIndex)].Term
		}
		args.Entries = rf.logs[rf.getActLogIndex(p):]
		// 请求不成功则退出，等待下一次 AppendEntries
		if !rf.sendAppendEntries(index, &args, &reply) {
			break
		}

		rf.mu.Lock()
		// 请求过程中，若等待过久，可能出现状态变更
		if rf.killed() != false || rf.state != leaderState || term != rf.curTerm ||
			rf.nextIndex[index] != nextIndex {
			rf.mu.Unlock()
			break
		}
		// 如果对方term更大，直接更新当前term，转为follower状态
		if reply.Term > rf.curTerm {
			DPrintf("[%d][term %d][%s] [%d]'s term is larger", rf.me, rf.curTerm, rf.state, index)
			rf.curTerm = reply.Term
			rf.becomeFollower()
			rf.persist()
			rf.mu.Unlock()
			break
		}
		// 复制日志条目成功：更新nextIndex[index] matchIndex[index] lastCommittedIndex
		if reply.Success {
			DPrintf("[%d][term %d][%s] update nextIndex[%d]: %d -> %d", rf.me, rf.curTerm, rf.state, index, rf.nextIndex[index], p+int32(len(args.Entries)))
			rf.nextIndex[index] = p + int32(len(args.Entries))
			rf.matchIndex[index] = rf.nextIndex[index] - 1
			rf.updateCommitIndex(rf.matchIndex[index])
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
		// 如果因为日志不一致失败，重试
		p = reply.ExpectNextIndex
		time.Sleep(10 * time.Millisecond)
	}
}

// updateCommitIndex 更新已提交记录索引
// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
// 如果日志条目被复制到大多数节点，且该条目的term是当前term，就标识它为已提交的
func (rf *Raft) updateCommitIndex(newCommitIndex int32) {
	// 如果 index 已提交，或 logs[index].Term 不是当前 term ，则不能提交
	// lastCommittedIndex 最小是0，且 lastCommittedIndex >= lastIncludedIndex
	if newCommitIndex <= rf.lastCommittedIndex ||
		rf.logs[rf.getActLogIndex(newCommitIndex)].Term != rf.curTerm {
		return
	}

	cnt := 0
	for i := 0; i < len(rf.matchIndex); i++ {
		// 自身也算
		if i == rf.me || rf.matchIndex[i] >= newCommitIndex {
			cnt++
		}
	}
	if cnt > len(rf.peers)/2 {
		DPrintf("[%d][term %d][%s] updateCommitIndex: %d -> %d", rf.me, rf.curTerm, rf.state, rf.lastCommittedIndex, newCommitIndex)
		rf.lastCommittedIndex = newCommitIndex
		// 向状态机服务发送消息，执行已提交记录中的命令
		applyMsgs := make([]ApplyMsg, 0)
		for rf.lastAppliedIndex < rf.lastCommittedIndex {
			rf.lastAppliedIndex++
			applyMsgs = append(applyMsgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.getActLogIndex(rf.lastAppliedIndex)].Command,
				CommandIndex: int(rf.lastAppliedIndex),
			})
		}
		// Note: 对外发送消息一律另开线程，防止一直占有锁
		go rf.applyLogEntries2service(applyMsgs)
	}
}

// installSnapshot2sever 向服务器<index>发送 InstallSnapshot 请求
func (rf *Raft) installSnapshot2sever(term int32, index int, nextIndex int32,
	lastIncludedIndex int32, lastIncludedTerm int32, data []byte) {
	var reply InstallSnapshotReply
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderId:          int32(rf.me),
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	}
	// 请求不成功，直接退出，等待下次
	if !rf.sendInstallSnapshot(index, &args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 请求过程中，若等待过久，可能出现状态变更
	if rf.state != leaderState || term != rf.curTerm ||
		rf.nextIndex[index] != nextIndex ||
		rf.lastIncludedIndex != lastIncludedIndex {
		return
	}
	// 如果对方term更大，直接更新当前term，转为follower状态
	if reply.Term > rf.curTerm {
		rf.curTerm = reply.Term
		rf.becomeFollower()
		rf.persist()
		return
	}
	// 更新nextIndex
	rf.nextIndex[index] = rf.lastIncludedIndex + 1
}
