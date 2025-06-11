package raft

import (
	"log"
	"fmt"
	"math/rand"
	"time"
)

const Debug = false
func DPrintf(format string, a ...any) {
	if Debug {
		log.Printf(format, a...)
	}
}

// Return the index of the log entry just before nextIndex[server]
func (rf *Raft) prevLogIndex(server int) int {
	return rf.nextIndex[server] - 1
}

// Return the term of the log entry just before nextIndex[server]
func (rf *Raft) prevLogTerm(server int) int {
	prevIndex := rf.prevLogIndex(server)
	if prevIndex < 0 {
		panic(fmt.Sprintf("server %d prevIndex %d < 0", server, prevIndex))
	}
	return rf.raftLog.EntryAt(prevIndex).Term
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Reset(time.Duration(750+rand.Int63()%750) * time.Millisecond)
}

func (rf *Raft) heartbeatTimerReset() {
	rf.heartbeatTimer.Reset(50 * time.Millisecond)
}

// shrinkLogFrom the log starting from index, keeping later entries
func (rf *Raft) shrinkLogFrom(index int) {
	start := index - rf.raftLog.FirstIndex()
	if start < 0 || start >= len(rf.raftLog) {
		panic("shrinkLogFrom: invalid shrink index")
	}

	newLog := append([]LogEntry(nil), rf.raftLog[start:]...)
	newLog[0].Command = nil // clear first dummy command
	rf.raftLog = newLog
}

// Reset the log to a new base entry at (firstIndex, firstTerm)
func (rf *Raft) renewLog(firstIndex, firstTerm int) {
	rf.raftLog = []LogEntry{{Command: nil, Term: firstTerm, Index: firstIndex}}
}