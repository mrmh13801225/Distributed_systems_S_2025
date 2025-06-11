package raft

import (
	"log"
	"fmt"
	"math/rand"
	"time"
	"sync/atomic"
	"bytes"
	"6.5840/labgob"
)

const Debug = false
func DPrintf(format string, a ...any) {
	if Debug {
		log.Printf(format, a...)
	}
}

func (rf *Raft) encodeState() []byte {
	var buffer bytes.Buffer
	encoder := labgob.NewEncoder(&buffer)

	if err := encoder.Encode(rf.currentTermID); err != nil {
		panic("failed to encode currentTerm")
	}
	if err := encoder.Encode(rf.votedFor); err != nil {
		panic("failed to encode votedFor")
	}
	if err := encoder.Encode(rf.raftLog); err != nil {
		panic("failed to encode log")
	}

	return buffer.Bytes()
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

func (rf *Raft) startElection() {
	rf.votedFor = rf.me
	rf.persist()

	var voteCount int32 = 1 // includes self-vote
	args := rf.genRequestVoteArgs()

	for peerID := range rf.peers {
		if peerID == rf.me {
			continue
		}

		go func(peer int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// Ignore stale term or role change
				if rf.state != Candidate || rf.currentTermID != args.Term {
					return
				}

				if reply.VoteGranted {
					newCount := atomic.AddInt32(&voteCount, 1)
					if int(newCount) >= (len(rf.peers) / 2) + 1 {
						rf.becomeLeader()
					}
				} else if reply.Term > rf.currentTermID {
					rf.becomeFollower(reply.Term)
				}
			}
		}(peerID)
	}
}

// State related Utils:

func (rf *Raft) becomeFollower(term int) {
	rf.currentTermID = term
	rf.votedFor = -1
	rf.persist()
	rf.state = Follower
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader

	lastIndex := rf.raftLog.LastIndex() + 1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex
		rf.matchIndex[i] = 0
	}

	rf.WakeAllAppender()
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTermID += 1
	rf.persist()
	rf.startElection()
}
