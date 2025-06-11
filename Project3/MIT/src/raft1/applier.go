package raft

import (
	"6.5840/raftapi"
)

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for !(rf.commitIndex > rf.lastApplied) {
			rf.applyCond.Wait()
		}
		if rf.lastApplied < rf.raftLog.FirstIndex() {
			rf.applySnapshotCommand()
		} else {
			rf.applyLogCommand()
		}
	}
}

func (rf *Raft) applyLogCommand() {
	rf.lastApplied += 1
	command := rf.raftLog.EntryAt(rf.lastApplied).Command
	if command == nil {
		return
	}
	index := rf.raftLog.EntryAt(rf.lastApplied).Index
	applyMag := raftapi.ApplyMsg{
		CommandValid: true,
		Command:      command,
		CommandIndex: index,
	}
	rf.mu.Unlock()
	select {
	case rf.applyCh <- applyMag:
	case <-rf.shutdownCh:
		close(rf.applyCh)
	}
}

func (rf *Raft) applySnapshotCommand() {
	rf.lastApplied = rf.raftLog.FirstIndex()
	applyMsg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.persister.ReadSnapshot(),
		SnapshotTerm:  rf.raftLog.FirstTerm(),
		SnapshotIndex: rf.raftLog.FirstIndex(),
	}
	rf.mu.Unlock()

	select {
	case rf.applyCh <- applyMsg:
	case <-rf.shutdownCh:
		close(rf.applyCh)
	}
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTermID:
// set commitIndex = N
func (rf *Raft) updateCommitIndex() {
	for N := rf.raftLog.LastIndex(); max(rf.commitIndex, rf.raftLog.FirstIndex()) < N; N-- {
		count := 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= N {
				count += 1
			}
		}
		if count >= (len(rf.peers)/2+1) && rf.raftLog.EntryAt(N).Term == rf.currentTermID {
			rf.commitIndex = N
			rf.applyCond.Signal()
			rf.broadcastHeartBeat()
			return
		}
	}
}
