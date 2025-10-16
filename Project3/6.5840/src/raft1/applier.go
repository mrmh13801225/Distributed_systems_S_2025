package raft

import (
	"sync/atomic"
	"6.5840/raftapi"
)

type ApplyStrategy interface {
	shouldApply(rf *Raft) bool
	createApplyMsg(rf *Raft) (*raftapi.ApplyMsg, error)
	updateState(rf *Raft)
}

type LogEntryStrategy struct{}

type SnapshotStrategy struct{}

func (rf *Raft) applier() {
	defer func() {
		if r := recover(); r != nil {
			DPrintf("Applier panic recovered: %v", r)
			atomic.AddInt64(&rf.applierMetrics.errors, 1)
		}
	}()

	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}

		strategy := rf.selectApplyStrategy()
		if strategy.shouldApply(rf) {
			rf.executeApplyStrategy(strategy)
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) selectApplyStrategy() ApplyStrategy {
	if rf.lastApplied < rf.logStore.FirstIndex() {
		return &SnapshotStrategy{}
	}
	return &LogEntryStrategy{}
}

func (rf *Raft) executeApplyStrategy(strategy ApplyStrategy) {
	applyMsg, err := strategy.createApplyMsg(rf)
	if err != nil {
		rf.mu.Unlock()
		atomic.AddInt64(&rf.applierMetrics.errors, 1)
		DPrintf("Error creating apply message: %v", err)
		return
	}

	strategy.updateState(rf)
	rf.mu.Unlock()

	if applyMsg != nil {
		rf.safelyApplyMessage(*applyMsg)
	}
}

func (s *LogEntryStrategy) shouldApply(rf *Raft) bool {
	return rf.lastApplied < rf.commitIndex
}

func (s *LogEntryStrategy) createApplyMsg(rf *Raft) (*raftapi.ApplyMsg, error) {
	nextIndex := rf.lastApplied + 1
	entry := rf.logStore.EntryAt(nextIndex)
	
	if entry.Command == nil {
		return nil, nil
	}

	return &raftapi.ApplyMsg{
		CommandValid: true,
		Command:      entry.Command,
		CommandIndex: entry.LogIndex,
	}, nil
}

func (s *LogEntryStrategy) updateState(rf *Raft) {
	rf.lastApplied++
	atomic.AddInt64(&rf.applierMetrics.entriesApplied, 1)
}

func (s *SnapshotStrategy) shouldApply(rf *Raft) bool {
	return rf.lastApplied < rf.logStore.FirstIndex()
}

func (s *SnapshotStrategy) createApplyMsg(rf *Raft) (*raftapi.ApplyMsg, error) {
	return &raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.persister.ReadSnapshot(),
		SnapshotTerm:  rf.logStore.FirstTerm(),
		SnapshotIndex: rf.logStore.FirstIndex(),
	}, nil
}

func (s *SnapshotStrategy) updateState(rf *Raft) {
	rf.lastApplied = rf.logStore.FirstIndex()
	atomic.AddInt64(&rf.applierMetrics.snapshotsApplied, 1)
}

func (rf *Raft) applyLogEntry() {
	strategy := &LogEntryStrategy{}
	if strategy.shouldApply(rf) {
		applyMsg, err := strategy.createApplyMsg(rf)
		if err != nil {
			rf.mu.Unlock()
			return
		}
		strategy.updateState(rf)
		rf.mu.Unlock()

		if applyMsg != nil {
			rf.safelyApplyMessage(*applyMsg)
		}
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) applySnapshot() {
	strategy := &SnapshotStrategy{}
	if strategy.shouldApply(rf) {
		applyMsg, _ := strategy.createApplyMsg(rf)
		strategy.updateState(rf)
		rf.mu.Unlock()

		rf.safelyApplyMessage(*applyMsg)
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) safelyApplyMessage(msg raftapi.ApplyMsg) {
	select {
	case rf.applyCh <- msg:
	case <-rf.shutdownCh:
		return
	}
}

func (rf *Raft) updateCommitIndex() {
	if rf.state != Leader {
		return
	}

	oldCommitIndex := rf.commitIndex

	for N := rf.logStore.LastIndex(); N > max(rf.commitIndex, rf.logStore.FirstIndex()); N-- {
		if N < rf.logStore.FirstIndex() {
			continue
		}
		
		if rf.logStore.EntryAt(N).TermNumber != rf.currentTermID {
			continue
		}

		count := 1 
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}

		if count >= (len(rf.peers)/2)+1 {
			rf.commitIndex = N
			rf.applyCond.Signal()
			break 
		}
	}


	if rf.commitIndex > oldCommitIndex {
		rf.broadcastHeartBeat()
	}
}
