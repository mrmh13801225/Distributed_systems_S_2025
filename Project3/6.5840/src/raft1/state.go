package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) becomeCandidate() {
	// defer rf.DPrintf("convert to condidate in term %d\n", rf.currentTermID)
	rf.role = StateCandidate
	rf.currentTermID += 1
	rf.persist()
	rf.startElection()
}

func (rf *Raft) becomeFollower(term int) {
	// defer rf.DPrintf("convert to StateFollower in term %d\n", rf.currentTermID)
	rf.currentTermID = term
	rf.lastVotedFor = -1
	rf.persist()
	rf.role = StateFollower
}

func (rf *Raft) becomeLeader() {
	// defer rf.DPrintf("convert to StateLeader in term %d\n", rf.currentTermID)
	rf.role = StateLeader
	rf.nextLogIdx = fillSlice(len(rf.peers), rf.entryLog.LastIndex()+1)
	rf.matchLogIdx = fillSlice(len(rf.peers), 0)
	// rf.AppendLog(nil)
	rf.wakeAppenders()
}

func (rf *Raft) resetVoteTimer() {
	rf.voteTimer.Reset(time.Duration(750 + rand.Int63() % 750) * time.Millisecond)
}

func (rf *Raft) resetPingTimer() {
	rf.pingTimer.Reset(50 * time.Millisecond)
}