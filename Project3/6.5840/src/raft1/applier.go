package raft

import (
	"sync/atomic"
	"6.5840/raftapi"
)

// ApplyStrategy defines the interface for different application strategies
type ApplyStrategy interface {
	shouldApply(rf *Raft) bool
	createApplyMsg(rf *Raft) (*raftapi.ApplyMsg, error)
	updateState(rf *Raft)
}

// LogEntryStrategy implements strategy for applying log entries
type LogEntryStrategy struct{}

// SnapshotStrategy implements strategy for applying snapshots  
type SnapshotStrategy struct{}

// Main applier goroutine - Template Method Pattern
func (rf *Raft) applier() {
	defer func() {
		if r := recover(); r != nil {
			DPrintf("Applier panic recovered: %v", r)
			// Increment error metric
			atomic.AddInt64(&rf.applierMetrics.errors, 1)
		}
	}()

	for !rf.killed() {
		// Wait for work - Producer-Consumer Pattern
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}

		// Strategy Pattern - choose appropriate application strategy
		strategy := rf.selectApplyStrategy()
		if strategy.shouldApply(rf) {
			rf.executeApplyStrategy(strategy)
		} else {
			rf.mu.Unlock()
		}
	}
}

// Factory method to select appropriate application strategy
func (rf *Raft) selectApplyStrategy() ApplyStrategy {
	if rf.lastApplied < rf.logStore.FirstIndex() {
		return &SnapshotStrategy{}
	}
	return &LogEntryStrategy{}
}

// Template method for executing application strategy
func (rf *Raft) executeApplyStrategy(strategy ApplyStrategy) {
	// Create apply message while holding lock
	applyMsg, err := strategy.createApplyMsg(rf)
	if err != nil {
		rf.mu.Unlock()
		atomic.AddInt64(&rf.applierMetrics.errors, 1)
		DPrintf("Error creating apply message: %v", err)
		return
	}

	// Update state
	strategy.updateState(rf)
	rf.mu.Unlock()

	// Send message without holding lock
	if applyMsg != nil {
		rf.safelyApplyMessage(*applyMsg)
	}
}

// LogEntryStrategy implementation
func (s *LogEntryStrategy) shouldApply(rf *Raft) bool {
	return rf.lastApplied < rf.commitIndex
}

func (s *LogEntryStrategy) createApplyMsg(rf *Raft) (*raftapi.ApplyMsg, error) {
	nextIndex := rf.lastApplied + 1
	entry := rf.logStore.EntryAt(nextIndex)
	
	// Skip dummy entries (command == nil)
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

// SnapshotStrategy implementation
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

// Legacy wrapper methods for backward compatibility
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
			rf.sendApplyMsg(*applyMsg)
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

		rf.sendApplyMsg(*applyMsg)
	} else {
		rf.mu.Unlock()
	}
}

// Safe message delivery with proper error handling
func (rf *Raft) safelyApplyMessage(msg raftapi.ApplyMsg) {
	select {
	case rf.applyCh <- msg:
		// Message sent successfully
	case <-rf.shutdownCh:
		// Server is shutting down
		return
	}
}

func (rf *Raft) sendApplyMsg(msg raftapi.ApplyMsg) {
	select {
	case rf.applyCh <- msg:
	case <-rf.shutdownCh:
		// Don't close channel here - let cleanup handle it
		return
	}
}

// Optimized commit index update with better performance
func (rf *Raft) updateCommitIndex() {
	if rf.state != Leader {
		return
	}

	oldCommitIndex := rf.commitIndex

	// Find the highest index that majority of servers have replicated
	// Use backward iteration like the original - safer with snapshots
	for N := rf.logStore.LastIndex(); N > max(rf.commitIndex, rf.logStore.FirstIndex()); N-- {
		// Skip entries that have been compacted away
		if N < rf.logStore.FirstIndex() {
			continue
		}
		
		// Only commit entries from current term (Raft safety requirement)
		if rf.logStore.EntryAt(N).TermNumber != rf.currentTermID {
			continue
		}

		count := 1 // Count self
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}

		// If majority has replicated this entry, commit it
		if count >= (len(rf.peers)/2)+1 {
			rf.commitIndex = N
			rf.applyCond.Signal()
			break // Found the highest committable index
		}
	}

	// Broadcast heartbeat to inform followers of commit updates
	// Only broadcast if we actually updated the commit index
	if rf.commitIndex > oldCommitIndex {
		rf.broadcastHeartBeat()
	}
}
