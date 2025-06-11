package raft

// TODO:: refactor

import (
	"6.5840/raftapi"
)

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}

		if rf.lastApplied < rf.raftLog.FirstIndex() {
			rf.applySnapshot()
		} else {
			rf.applyLogEntry()
		}
	}
}

func (rf *Raft) applyLogEntry() {
	rf.lastApplied++
	entry := rf.raftLog.EntryAt(rf.lastApplied)

	if entry.Command == nil {
		rf.mu.Unlock()
		return
	}

	applyMsg := raftapi.ApplyMsg{
		CommandValid: true,
		Command:      entry.Command,
		CommandIndex: entry.LogIndex,
	}
	rf.mu.Unlock()

	rf.sendApplyMsg(applyMsg)
}

func (rf *Raft) sendApplyMsg(msg raftapi.ApplyMsg) {
	select {
	case rf.applyCh <- msg:
	case <-rf.shutdownCh:
		close(rf.applyCh)
	}
}

func (rf *Raft) applySnapshot() {
	rf.lastApplied = rf.raftLog.FirstIndex()

	applyMsg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.persister.ReadSnapshot(),
		SnapshotTerm:  rf.raftLog.FirstTerm(),
		SnapshotIndex: rf.raftLog.FirstIndex(),
	}
	rf.mu.Unlock()

	rf.sendApplyMsg(applyMsg)
}

func (rf *Raft) updateCommitIndex() {
	for N := rf.raftLog.LastIndex(); N > max(rf.commitIndex, rf.raftLog.FirstIndex()); N-- {
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}

		if count >= (len(rf.peers)/2+1) && rf.raftLog.EntryAt(N).TermNumber == rf.currentTermID {
			rf.commitIndex = N
			rf.applyCond.Signal()
			rf.broadcastHeartBeat()
			return
		}
	}
}
