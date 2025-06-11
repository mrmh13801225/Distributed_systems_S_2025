package raft

import (
	"sync/atomic"
)

func (rf *Raft) becomeFollower(term int) {
	rf.currentTermID = term
	rf.votedFor = -1
	rf.persist()
	rf.state = Follower
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.nextIndex = fillSlice(len(rf.peers), rf.raftLog.LastIndex()+1)
	rf.matchIndex = fillSlice(len(rf.peers), 0)
	rf.WakeAllAppender()
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTermID += 1
	rf.persist()
	rf.startElection()
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
					if int(newCount) >= len(rf.peers)/2+1 {
						rf.becomeLeader()
					}
				} else if reply.Term > rf.currentTermID {
					rf.becomeFollower(reply.Term)
				}
			}
		}(peerID)
	}
}
