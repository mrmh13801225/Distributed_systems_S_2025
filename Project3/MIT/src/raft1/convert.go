package raft

func (rf *Raft) convertToFollower(term int) {
	defer rf.DPrintf("convert to Follower in term %d\n", rf.currentTermID)

	rf.currentTermID = term
	rf.votedFor = -1
	rf.persist()

	rf.state = Follower
}

func (rf *Raft) convertToLeader() {
	defer rf.DPrintf("convert to Leader in term %d\n", rf.currentTermID)

	rf.state = Leader
	rf.nextIndex = fillSlice(len(rf.peers), rf.raftLog.LastIndex()+1)
	rf.matchIndex = fillSlice(len(rf.peers), 0)
	// rf.AppendLog(nil)
	rf.WakeAllAppender()
}

func (rf *Raft) convertToCondidate() {
	defer rf.DPrintf("convert to condidate in term %d\n", rf.currentTermID)

	rf.state = Candidate
	rf.currentTermID += 1
	rf.persist()

	// start election
	rf.startElection()
}

func (rf *Raft) startElection() {
	rf.votedFor = rf.me
	rf.persist()

	numVotes := 1

	args := rf.genRequestVoteArgs()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(server, args, reply) {
				rf.mu.Lock()
				rf.DPrintf("send request vote to %d\n", server)
				if rf.state != Candidate || rf.currentTermID != args.Term {
					rf.mu.Unlock()
					return
				}

				if reply.VoteGranted {
					numVotes += 1
					if numVotes >= (len(rf.peers)/2 + 1) {
						rf.convertToLeader()
						numVotes = -99 // prevent double conversion
					}
				} else if reply.Term > rf.currentTermID {
					rf.convertToFollower(reply.Term)
				}

				rf.mu.Unlock()
			}
		}(server)
	}
}
