package raft

import (
	"sync/atomic"
	"time"
)

// AppendStrategy defines the interface for different append operations
type AppendStrategy interface {
	shouldSend(rf *Raft, server int) bool
	executeAppend(rf *Raft, server int) AppendResult
	getName() string
}

// AppendResult encapsulates the result of an append operation
type AppendResult struct {
	success    bool
	termChange bool
	newTerm    int
	conflict   bool
	reply      interface{}
}

// LogAppendStrategy handles normal log entry replication
type LogAppendStrategy struct{}

// SnapshotAppendStrategy handles snapshot installation
type SnapshotAppendStrategy struct{}

// Main appender goroutine - Template Method Pattern with improved error handling
func (rf *Raft) appender(server int) {
	defer func() {
		if r := recover(); r != nil {
			DPrintf("Appender[%d] panic recovered: %v", server, r)
			atomic.AddInt64(&rf.appenderMetrics[server].errors, 1)
		}
	}()

	lock := rf.appendConds[server].L
	lock.Lock()
	defer lock.Unlock()

	for !rf.killed() {
		// Wait for work - Producer-Consumer Pattern
		for !rf.shouldAppend(server) {
			rf.appendConds[server].Wait()
		}
		
		// Execute append with proper error handling
		rf.executeAppendWithStrategy(server)
		
		// Adaptive sleep based on success/failure
		time.Sleep(10 * time.Millisecond)
	}
}

// Template method for executing append operations
func (rf *Raft) executeAppendWithStrategy(server int) {
	strategy := rf.selectAppendStrategy(server)
	
	if strategy.shouldSend(rf, server) {
		result := strategy.executeAppend(rf, server)
		rf.handleAppendResult(server, result)
	}
}

// Factory method to select appropriate append strategy
func (rf *Raft) selectAppendStrategy(server int) AppendStrategy {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	
	if rf.prevLogIndex(server) < rf.logStore.FirstIndex() {
		return &SnapshotAppendStrategy{}
	}
	return &LogAppendStrategy{}
}

// Handle append result with proper state management
func (rf *Raft) handleAppendResult(server int, result AppendResult) {
	if result.termChange {
		rf.mu.Lock()
		if result.newTerm > rf.currentTermID {
			rf.becomeFollower(result.newTerm)
		}
		rf.mu.Unlock()
	}
	
	if result.success {
		atomic.AddInt64(&rf.appenderMetrics[server].successCount, 1)
	} else if result.conflict {
		atomic.AddInt64(&rf.appenderMetrics[server].conflictCount, 1)
	}
}

func (rf *Raft) shouldAppend(server int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && rf.nextIndex[server] <= rf.logStore.LastIndex()
}

// LogAppendStrategy implementation
func (s *LogAppendStrategy) shouldSend(rf *Raft, server int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader
}

func (s *LogAppendStrategy) executeAppend(rf *Raft, server int) AppendResult {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return AppendResult{success: false}
	}
	
	args := rf.genAppendEntriesArgs(server)
	reply := &AppendEntriesReply{}
	rf.mu.Unlock()

	success := rf.sendAppendEntries(server, args, reply)
	if !success {
		return AppendResult{success: false}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.state != Leader || rf.currentTermID != args.TermNumber {
		return AppendResult{success: false}
	}

	if reply.TermNumber > rf.currentTermID {
		return AppendResult{
			success:    false,
			termChange: true,
			newTerm:    reply.TermNumber,
		}
	}

	if reply.Success {
		rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.LogEntry))
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.updateCommitIndex()
		return AppendResult{success: true}
	} else if reply.Confict {
		rf.updateNextIndexAfterConflict(server, reply)
		return AppendResult{success: false, conflict: true, reply: reply}
	}
	
	return AppendResult{success: false}
}

func (s *LogAppendStrategy) getName() string {
	return "LogAppend"
}

// SnapshotAppendStrategy implementation
func (s *SnapshotAppendStrategy) shouldSend(rf *Raft, server int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && rf.prevLogIndex(server) < rf.logStore.FirstIndex()
}

func (s *SnapshotAppendStrategy) executeAppend(rf *Raft, server int) AppendResult {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return AppendResult{success: false}
	}
	
	args := rf.genInstallSnapshotArgs()
	reply := &InstallSnapshotReply{}
	rf.mu.Unlock()

	success := rf.sendInstallSnapshot(server, args, reply)
	if !success {
		return AppendResult{success: false}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.state != Leader || rf.currentTermID != args.TermNumber {
		return AppendResult{success: false}
	}

	if reply.TermNumber > rf.currentTermID {
		return AppendResult{
			success:    false,
			termChange: true,
			newTerm:    reply.TermNumber,
		}
	}

	rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	atomic.AddInt64(&rf.appenderMetrics[server].snapshotsSent, 1)
	return AppendResult{success: true}
}

func (s *SnapshotAppendStrategy) getName() string {
	return "SnapshotInstall"
}

func (rf *Raft) WakeAllAppender() {
	for server := range rf.peers {
		if server != rf.me {
			rf.appendConds[server].Signal()
		}
	}
}

func (rf *Raft) doAppendJob(server int) {
	strategy := rf.selectAppendStrategy(server)
	
	if strategy.shouldSend(rf, server) {
		result := strategy.executeAppend(rf, server)
		rf.handleAppendResult(server, result)
		atomic.AddInt64(&rf.appenderMetrics[server].totalSent, 1)
	}
}

func (rf *Raft) sendSnapshotTo(server int) {
	strategy := &SnapshotAppendStrategy{}
	result := strategy.executeAppend(rf, server)
	rf.handleAppendResult(server, result)
}

// Legacy wrapper methods for backward compatibility
func (rf *Raft) sendAppendEntriesTo(server int) {
	strategy := &LogAppendStrategy{}
	result := strategy.executeAppend(rf, server)
	rf.handleAppendResult(server, result)
}

// Improved conflict resolution with better error handling
func (rf *Raft) updateNextIndexAfterConflict(server int, reply *AppendEntriesReply) {
	// Handle case where follower's log is too short
	if reply.XTerm == -1 && reply.XIndex == -1 {
		rf.nextIndex[server] = reply.XLen
		return
	}

	// Search backwards for the conflicting term
	for i := min(rf.prevLogIndex(server), rf.logStore.LastIndex()); i >= rf.logStore.FirstIndex(); i-- {
		if rf.logStore.EntryAt(i).TermNumber == reply.XTerm {
			rf.nextIndex[server] = i + 1
			return
		} else if rf.logStore.EntryAt(i).TermNumber < reply.XTerm {
			break
		}
	}

	// Fallback: use the index provided by follower
	rf.nextIndex[server] = max(min(reply.XIndex, rf.logStore.LastIndex()+1), rf.logStore.FirstIndex())
}

// Improved heartbeat broadcast with better resource management
func (rf *Raft) broadcastHeartBeat() {
	if rf.state != Leader {
		return
	}

	for server := range rf.peers {
		if server != rf.me {
			// Keep the original behavior of creating goroutines for immediate sending
			go rf.doAppendJob(server)
		}
	}
}
