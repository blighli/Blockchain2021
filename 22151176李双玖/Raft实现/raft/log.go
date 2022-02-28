package raft

import (
	"fmt"
	"log"
)

type LogEntry struct {
	Index   int32
	Term    int32
	Command interface{}
}

func (e *LogEntry) String() string {
	return fmt.Sprintf("{Index:%d, Term:%d, Command:%+v}", e.Index, e.Term, e.Command)
}

// applyLogEntries2service 向状态机服务发送消息，执行已提交记录中的命令
func (rf *Raft) applyLogEntries2service(applyMsgs []ApplyMsg) {
	for i := 0; i < len(applyMsgs); i++ {
		DPrintf("[%d][term %d][%s] apply logs[%d], ApplyMsg: %+v", rf.me, rf.curTerm, rf.state, applyMsgs[i].CommandIndex, applyMsgs[i])
		// Bug: 接受applyCh消息的service会调用raft.Snapshot()，导致死锁
		rf.applyCh <- applyMsgs[i]
	}
}

// getActLogIndex 获取实际的索引值
func (rf *Raft) getActLogIndex(index int32) int32 {
	if index == 0 {
		log.Fatalf("[%d][term %d][%s] getActLogIndex: index == 0", rf.me, rf.curTerm, rf.state)
	}
	if index <= rf.lastIncludedIndex {
		log.Fatalf("[%d][term %d][%s] getActLogIndex: index(%d) <= rf.lastIncludedIndex(%d)", rf.me, rf.curTerm, rf.state, index, rf.lastIncludedIndex)
	}
	return index - rf.lastIncludedIndex - 1
}

// getLastLogIndex 获取日志最后条目的索引
func (rf *Raft) getLastLogIndex() int32 {
	return rf.lastIncludedIndex + int32(len(rf.logs))
}

// getLastLogTerm 获取日志最后条目的Term值
func (rf *Raft) getLastLogTerm() int32 {
	if rf.getLastLogIndex() == 0 {
		return -1
	} else if len(rf.logs) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.logs[rf.getActLogIndex(rf.getLastLogIndex())].Term
}
