package raft

func (rf *Raft) becomeFollower() {
	rf.state = followerState
	rf.votedFor = -1
}
