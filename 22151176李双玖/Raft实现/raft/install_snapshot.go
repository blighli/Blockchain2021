package raft

import (
	"math"
	"math/rand"
)

type InstallSnapshotArgs struct {
	Flag              int32  // 随机数，用于标识当前请求
	Term              int32  // leader的term号
	LeaderId          int32  // leader ID
	LastIncludedIndex int32  // 快照的最后一个条目的索引
	LastIncludedTerm  int32  // 快照的最后一个条目的term号
	Data              []byte // 快照
}

type InstallSnapshotReply struct {
	Term int32 // 接收者的term号
}

// InstallSnapshot 处理 InstallSnapshotRPC 请求
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("[%d][term %d][%s] InstallSnapshot from [%d] <%d> args ==> %+v", rf.me, rf.curTerm, rf.state, args.LeaderId, args.Flag, args)
	defer DPrintf("[%d][term %d][%s] InstallSnapshot from [%d] <%d> reply <== %+v", rf.me, rf.curTerm, rf.state, args.LeaderId, args.Flag, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 对方 term 过期直接拒绝
	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm
		return
	}

	// 由于 InstallSnapshot 请求在 AppendEntries 请求之后才会发送，因此 term 不会大于对方的

	// 将新的快照告诉状态机
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			SnapshotIndex: int(args.LastIncludedIndex),
			SnapshotTerm:  int(args.LastIncludedTerm),
			Snapshot:      args.Data,
		}
	}()
}

// sendInstallSnapshot 发送 InstallSnapshotRPC 请求
func (rf *Raft) sendInstallSnapshot(server int,
	args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	args.Flag = rand.Int31n(math.MaxInt32)
	DPrintf("[%d][term %d][%s] InstallSnapshot to [%d] <%d> args ==> %+v", rf.me, rf.curTerm, rf.state, server, args.Flag, args)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	DPrintf("[%d][term %d][%s] InstallSnapshot to [%d] <%d> reply %v <== %+v", rf.me, rf.curTerm, rf.state, server, args.Flag, ok, reply)
	return ok
}
