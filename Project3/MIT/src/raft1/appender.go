package raft

// TODO:: refactor

import (
	"time"
)

// If last log index ≥ nextIndex for a follower: send
// AppendEntries RPC with log entries starting at nextIndex
// • If successful: update nextIndex and matchIndex for
// follower (§5.3)
// • If AppendEntries fails because of log inconsistency:
// decrement nextIndex and retry (§5.3)

func (rf *Raft) appender(server int) {
	lock := rf.appendCond[server].L
	lock.Lock()
	defer lock.Unlock()

	for !rf.killed() {
		for !rf.shouldAppend(server) {
			rf.appendCond[server].Wait()
		}
		rf.doAppendJob(server)
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) shouldAppend(server int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && rf.nextIndex[server] <= rf.raftLog.LastIndex()
}


func (rf *Raft) WakeAllAppender() {
	for server := range rf.peers {
		if server != rf.me {
			rf.appendCond[server].Signal()
		}
	}
}

func (rf *Raft) doAppendJob(server int) {
	rf.mu.Lock()
	defer func() {
		if rf.mu.TryLock() {
			rf.mu.Unlock()
		}
	}()

	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	if rf.prevLogIndex(server) < rf.raftLog.FirstIndex() {
		rf.sendSnapshotTo(server)
	} else {
		rf.sendAppendEntriesTo(server)
	}
}

func (rf *Raft) sendSnapshotTo(server int) {
	args := rf.genInstallSnapshotArgs()
	reply := &InstallSnapshotReply{}
	rf.mu.Unlock()

	if rf.sendInstallSnapshot(server, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Leader || rf.currentTermID != args.TermNumber {
			return
		}

		if reply.TermNumber > rf.currentTermID {
			rf.becomeFollower(reply.TermNumber)
			return
		}

		rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	}
}

func (rf *Raft) sendAppendEntriesTo(server int) {
	args := rf.genAppendEntriesArgs(server)
	reply := &AppendEntriesReply{}
	rf.mu.Unlock()

	if rf.sendAppendEntries(server, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state != Leader || rf.currentTermID != args.TermNumber {
			return
		}

		if reply.TermNumber > rf.currentTermID {
			rf.becomeFollower(reply.TermNumber)
			return
		}

		if reply.Success {
			rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.LogEntry))
			rf.nextIndex[server] = rf.matchIndex[server] + 1

			rf.updateCommitIndex()
		} else if reply.Confict {
			// rf.updateNextIndexAfterConflict(server, reply.XTerm, reply.XIndex, reply.XLen)
			rf.updateNextIndexAfterConflict(server, reply)
		}
	}
}

func (rf *Raft) updateNextIndexAfterConflict(server int, reply *AppendEntriesReply) {
	if reply.XTerm == -1 && reply.XIndex == -1 {
		rf.nextIndex[server] = reply.XLen
		return
	}

	for i := min(rf.prevLogIndex(server), rf.raftLog.LastIndex()); i >= rf.raftLog.FirstIndex(); i-- {
		if rf.raftLog.EntryAt(i).TermNumber == reply.XTerm {
			rf.nextIndex[server] = i + 1
			return
		} else if rf.raftLog.EntryAt(i).TermNumber < reply.XTerm {
			break
		}
	}

	rf.nextIndex[server] = max(min(reply.XIndex, rf.raftLog.LastIndex()+1), rf.raftLog.FirstIndex())
}

func (rf *Raft) broadcastHeartBeat() {
	if rf.state != Leader {
		return
	}

	for server := range rf.peers {
		if server != rf.me {
			go rf.doAppendJob(server)
		}
	}
}
