package raft

import (
	"sync/atomic"
)

// RPCHandler defines the interface for RPC request processing
type RPCHandler interface {
	validateRequest(rf *Raft) RPCValidationResult
	processRequest(rf *Raft) RPCProcessResult
	buildResponse(rf *Raft, result RPCProcessResult) interface{}
}

// RPCValidationResult encapsulates RPC validation outcome
type RPCValidationResult struct {
	valid      bool
	shouldProcess bool
	termChange bool
	newTerm    int
}

// RPCProcessResult encapsulates RPC processing outcome
type RPCProcessResult struct {
	success    bool
	conflict   bool
	termChange bool
	newTerm    int
	details    map[string]interface{}
}



// Template method for RPC processing with error handling
func (rf *Raft) processRPCWithHandler(handler RPCHandler, rpcType string) interface{} {
	defer func() {
		if r := recover(); r != nil {
			DPrintf("RPC %s panic recovered: %v", rpcType, r)
			atomic.AddInt64(&rf.rpcMetrics.appendEntriesReceived, 1) // Generic error counter
		}
	}()

	// Validation phase
	validationResult := handler.validateRequest(rf)
	if !validationResult.valid {
		return handler.buildResponse(rf, RPCProcessResult{success: false})
	}

	// State transition if needed
	if validationResult.termChange {
		rf.becomeFollower(validationResult.newTerm)
	}

	if !validationResult.shouldProcess {
		return handler.buildResponse(rf, RPCProcessResult{success: false})
	}

	// Processing phase
	processResult := handler.processRequest(rf)
	
	// Build and return response
	return handler.buildResponse(rf, processResult)
}

// Improved log up-to-date check with better documentation
func (rf *Raft) isCandidateLogUpToDate(args *RequestVoteArgs) bool {
	// Election restriction: candidate's log must be at least as up-to-date
	// as any other log in the majority that grants votes
	if rf.logStore.LastTerm() != args.LastLogTerm {
		return args.LastLogTerm > rf.logStore.LastTerm()
	}
	return args.LastLogIndex >= rf.logStore.LastIndex()
}

func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	return &RequestVoteArgs{
		TermNumber:         rf.currentTermID,
		CandidateId:  rf.me,
		LastLogIndex: rf.logStore.LastIndex(),
		LastLogTerm:  rf.logStore.LastTerm(),
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

type AppendEntriesArgs struct {
	TermNumber         int // leader’s term
	PrevLogIndex int // index of log entry immediately preceding new ones

	PrevLogTerm int       // term of prevLogIndex entry
	LogEntry     []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)

	LeaderCommit int // leader’s commitIndex
}

type AppendEntriesReply struct {
	TermNumber    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	Confict bool
	XTerm   int // term in the conflicting entry (if any)
	XIndex  int // index of first entry with that term (if any)
	XLen    int // log length
}

type InstallSnapshotArgs struct {
	TermNumber              int    // leader’s term
	LastIncludedIndex int    // snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of LastIncludedIndex
	Data              []byte // raw bytes of snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	TermNumber int // currentTerm, for leader to update itself
}

// Refactored AppendEntries RPC handler using Template Method Pattern
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	atomic.AddInt64(&rf.rpcMetrics.appendEntriesReceived, 1)
	
	handler := NewAppendEntriesHandler(args, reply)
	result := rf.processRPCWithHandler(handler, "AppendEntries")
	
	if result.(*AppendEntriesReply).Success {
		atomic.AddInt64(&rf.rpcMetrics.appendEntriesSucceeded, 1)
	}
}

func (rf *Raft) genAppendEntriesArgs(server int) *AppendEntriesArgs {
	prevIndex := rf.prevLogIndex(server)
	
	// Safety check: if prevIndex is before our log, this should use snapshot strategy instead
	if prevIndex < rf.logStore.FirstIndex() {
		DPrintf("Warning: genAppendEntriesArgs called with prevIndex %d < firstIndex %d for server %d", 
			prevIndex, rf.logStore.FirstIndex(), server)
		// Return minimal args to avoid crash - this shouldn't happen with correct strategy selection
		return &AppendEntriesArgs{
			TermNumber:         rf.currentTermID,
			PrevLogIndex: rf.logStore.FirstIndex() - 1,
			PrevLogTerm:  rf.logStore.FirstTerm(),
			LogEntry:     []LogEntry{},
			LeaderCommit: rf.commitIndex,
		}
	}
	
	return &AppendEntriesArgs{
		TermNumber:         rf.currentTermID,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  rf.prevLogTerm(server),
		LogEntry:     rf.logStore.EntriesInRange(prevIndex+1, rf.logStore.LastIndex()+1),
		LeaderCommit: rf.commitIndex,
	}
}

// Refactored InstallSnapshot RPC handler using Template Method Pattern
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	atomic.AddInt64(&rf.rpcMetrics.snapshotInstallReceived, 1)
	
	handler := NewInstallSnapshotHandler(args, reply)
	result := rf.processRPCWithHandler(handler, "InstallSnapshot")
	
	if result.(*InstallSnapshotReply) != nil {
		atomic.AddInt64(&rf.rpcMetrics.snapshotInstallSucceeded, 1)
	}
}

func (rf *Raft) genInstallSnapshotArgs() *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		TermNumber:              rf.currentTermID,
		LastIncludedIndex: rf.logStore.FirstIndex(),
		LastIncludedTerm:  rf.logStore.FirstTerm(),
		Data:              rf.persister.ReadSnapshot(),
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// AppendEntriesHandler implements the strategy for AppendEntries RPC
type AppendEntriesHandler struct {
	args  *AppendEntriesArgs
	reply *AppendEntriesReply
}

// NewAppendEntriesHandler creates a new AppendEntries handler
func NewAppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) *AppendEntriesHandler {
	return &AppendEntriesHandler{args: args, reply: reply}
}

func (h *AppendEntriesHandler) validateRequest(rf *Raft) RPCValidationResult {
	h.reply.TermNumber = rf.currentTermID
	h.reply.Success = false
	h.reply.Confict = false

	// Reject if term is outdated
	if h.args.TermNumber < rf.currentTermID {
		return RPCValidationResult{valid: true, shouldProcess: false}
	}

	// Update term and become follower if necessary
	needStateChange := h.args.TermNumber > rf.currentTermID || 
		(h.args.TermNumber == rf.currentTermID && rf.state == Candidate)

	if needStateChange {
		return RPCValidationResult{
			valid: true, 
			shouldProcess: true, 
			termChange: true, 
			newTerm: h.args.TermNumber,
		}
	}

	return RPCValidationResult{valid: true, shouldProcess: true}
}

func (h *AppendEntriesHandler) processRequest(rf *Raft) RPCProcessResult {
	// Reset election timer - valid leader communication
	rf.resetElectionTimer()

	// Check if previous log index is before our first index (snapshot case)
	if h.args.PrevLogIndex < rf.logStore.FirstIndex() {
		return RPCProcessResult{success: false}
	}

	// Check if we don't have the previous log entry
	if h.args.PrevLogIndex > rf.logStore.LastIndex() {
		return RPCProcessResult{
			success: false,
			conflict: true,
			details: map[string]interface{}{
				"XTerm":  -1,
				"XIndex": -1,
				"XLen":   rf.logStore.LastIndex() + 1,
			},
		}
	}

	// Check if previous log entry term matches
	if rf.logStore.EntryAt(h.args.PrevLogIndex).TermNumber != h.args.PrevLogTerm {
		return h.handleLogConflict(rf)
	}

	// Append new entries and handle conflicts
	h.handleLogAppend(rf)

	// Update commit index if leader's commit is higher
	if h.args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(h.args.LeaderCommit, rf.logStore.LastIndex())
		rf.applyCond.Signal()
	}

	return RPCProcessResult{success: true}
}

func (h *AppendEntriesHandler) handleLogConflict(rf *Raft) RPCProcessResult {
	term := rf.logStore.EntryAt(h.args.PrevLogIndex).TermNumber
	
	// Find first index of conflicting term
	i := h.args.PrevLogIndex - 1
	for i >= rf.logStore.FirstIndex() && rf.logStore.EntryAt(i).TermNumber == term {
		i--
	}

	return RPCProcessResult{
		success: false,
		conflict: true,
		details: map[string]interface{}{
			"XTerm":  term,
			"XIndex": i + 1,
			"XLen":   rf.logStore.LastIndex() + 1,
		},
	}
}

func (h *AppendEntriesHandler) handleLogAppend(rf *Raft) {
	start := h.args.PrevLogIndex + 1 - rf.logStore.FirstIndex()
	
	for i, entry := range h.args.LogEntry {
		if start+i >= len(rf.logStore) || rf.logStore[start+i].TermNumber != entry.TermNumber {
			// Conflict found - replace from this point onward
			rf.logStore = append(rf.logStore[:start+i], h.args.LogEntry[i:]...)
			rf.persist()
			break
		}
	}
}

func (h *AppendEntriesHandler) buildResponse(rf *Raft, result RPCProcessResult) interface{} {
	h.reply.Success = result.success
	h.reply.Confict = result.conflict

	if result.conflict && result.details != nil {
		if xterm, ok := result.details["XTerm"].(int); ok {
			h.reply.XTerm = xterm
		}
		if xindex, ok := result.details["XIndex"].(int); ok {
			h.reply.XIndex = xindex
		}
		if xlen, ok := result.details["XLen"].(int); ok {
			h.reply.XLen = xlen
		}
	}

	return h.reply
}

// InstallSnapshotHandler implements the strategy for InstallSnapshot RPC
type InstallSnapshotHandler struct {
	args  *InstallSnapshotArgs
	reply *InstallSnapshotReply
}

// NewInstallSnapshotHandler creates a new InstallSnapshot handler
func NewInstallSnapshotHandler(args *InstallSnapshotArgs, reply *InstallSnapshotReply) *InstallSnapshotHandler {
	return &InstallSnapshotHandler{args: args, reply: reply}
}

func (h *InstallSnapshotHandler) validateRequest(rf *Raft) RPCValidationResult {
	h.reply.TermNumber = rf.currentTermID
	
	// Reject if term is outdated
	if h.args.TermNumber < rf.currentTermID {
		return RPCValidationResult{valid: true, shouldProcess: false}
	}

	// Update term and become follower if necessary
	needStateChange := h.args.TermNumber > rf.currentTermID || 
		(h.args.TermNumber == rf.currentTermID && rf.state == Candidate)

	if needStateChange {
		return RPCValidationResult{
			valid: true, 
			shouldProcess: true, 
			termChange: true, 
			newTerm: h.args.TermNumber,
		}
	}

	return RPCValidationResult{valid: true, shouldProcess: true}
}

func (h *InstallSnapshotHandler) processRequest(rf *Raft) RPCProcessResult {
	// Reset election timer - valid leader communication
	rf.resetElectionTimer()

	// If we already have committed this snapshot or later, ignore it
	if rf.commitIndex >= h.args.LastIncludedIndex {
		return RPCProcessResult{success: true} // Not an error, just already applied
	}

	// Install the snapshot
	rf.renewLog(h.args.LastIncludedIndex, h.args.LastIncludedTerm)
	rf.persister.Save(rf.encodeState(), h.args.Data)
	rf.commitIndex = rf.logStore.FirstIndex()

	return RPCProcessResult{success: true}
}

func (h *InstallSnapshotHandler) buildResponse(rf *Raft, result RPCProcessResult) interface{} {
	return h.reply
}

// RequestVoteHandler implements the strategy for RequestVote RPC
type RequestVoteHandler struct {
	args  *RequestVoteArgs
	reply *RequestVoteReply
}

// NewRequestVoteHandler creates a new RequestVote handler
func NewRequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) *RequestVoteHandler {
	return &RequestVoteHandler{args: args, reply: reply}
}

func (h *RequestVoteHandler) validateRequest(rf *Raft) RPCValidationResult {
	h.reply.TermNumber = rf.currentTermID
	h.reply.VoteGranted = false

	// Reject if term is outdated
	if h.args.TermNumber < rf.currentTermID {
		return RPCValidationResult{valid: true, shouldProcess: false}
	}

	// Update term and become follower if necessary
	if h.args.TermNumber > rf.currentTermID {
		return RPCValidationResult{
			valid: true, 
			shouldProcess: true, 
			termChange: true, 
			newTerm: h.args.TermNumber,
		}
	}

	return RPCValidationResult{valid: true, shouldProcess: true}
}

func (h *RequestVoteHandler) processRequest(rf *Raft) RPCProcessResult {
	// Check if we can grant the vote
	canGrantVote := rf.currentTermID == h.args.TermNumber && 
		(rf.votedFor == -1 || rf.votedFor == h.args.CandidateId) && 
		rf.isCandidateLogUpToDate(h.args)

	if canGrantVote {
		rf.votedFor = h.args.CandidateId
		rf.persist()
		rf.resetElectionTimer()
		return RPCProcessResult{success: true}
	}

	return RPCProcessResult{success: false}
}

func (h *RequestVoteHandler) buildResponse(rf *Raft, result RPCProcessResult) interface{} {
	h.reply.VoteGranted = result.success
	return h.reply
}
