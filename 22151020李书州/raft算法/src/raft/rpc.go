package raft

import "fmt"

type RequestVoteRequest struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (request RequestVoteRequest) String() string {
	return fmt.Sprintf("{Term:%v,CandidateId:%v,LastLogIndex:%v,LastLogTerm:%v}", request.Term, request.CandidateId, request.LastLogIndex, request.LastLogTerm)
}

type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

func (response RequestVoteResponse) String() string {
	return fmt.Sprintf("{Term:%v,VoteGranted:%v}", response.Term, response.VoteGranted)
}

type AppendEntriesRequest struct {
	Term         int     // leader 任期
	LeaderId     int     // leader id
	PrevLogIndex int     // leader 中上一次同步的日志索引
	PrevLogTerm  int     // leader 中上一次同步的日志任期
	LeaderCommit int     // 领导者的已知已提交的最高的日志条目的索引
	Entries      []Entry // 同步日志
}

func (request AppendEntriesRequest) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v,PrevLogIndex:%v,PrevLogTerm:%v,LeaderCommit:%v,Entries:%v}", request.Term, request.LeaderId, request.PrevLogIndex, request.PrevLogTerm, request.LeaderCommit, request.Entries)
}

type AppendEntriesResponse struct {
	Term          int  // 当前任期号，以便于候选人去更新自己的任期号
	Success       bool // 是否同步成功，true 为成功
	ConflictIndex int  // 冲突index
	ConflictTerm  int  // 冲突Term
}

func (response AppendEntriesResponse) String() string {
	return fmt.Sprintf("{Term:%v,Success:%v,ConflictIndex:%v,ConflictTerm:%v}", response.Term, response.Success, response.ConflictIndex, response.ConflictTerm)
}

type InstallSnapshotRequest struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int //
	LastIncludedTerm  int
	Data              []byte
}

func (request InstallSnapshotRequest) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v,LastIncludedIndex:%v,LastIncludedTerm:%v,DataSize:%v}", request.Term, request.LeaderId, request.LastIncludedIndex, request.LastIncludedTerm, len(request.Data))
}

type InstallSnapshotResponse struct {
	Term int
}

func (response InstallSnapshotResponse) String() string {
	return fmt.Sprintf("{Term:%v}", response.Term)
}
