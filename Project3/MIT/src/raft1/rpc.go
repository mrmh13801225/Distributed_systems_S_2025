package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).

	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).

	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTermID

	if args.Term < rf.currentTermID {
		reply.Term = rf.currentTermID
		reply.VoteGranted = false
		return
	}

	if args.Term == rf.currentTermID && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTermID {
		rf.convertToFollower(args.Term)
	}

	if rf.candidateLogUptodate(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.electionTimerReset()
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) candidateLogUptodate(args *RequestVoteArgs) bool {
	if rf.raftLog.LastTerm() < args.LastLogTerm {
		return true
	} else if rf.raftLog.LastTerm() > args.LastLogTerm {
		return false
	} else {
		return rf.raftLog.LastIndex() <= args.LastLogIndex
	}
}

func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:         rf.currentTermID,
		CandidateId:  rf.me,
		LastLogIndex: rf.raftLog.LastIndex(),
		LastLogTerm:  rf.raftLog.LastTerm(),
	}
	return args
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
	Term         int // leader’s term
	PrevLogIndex int // index of log entry immediately preceding new ones

	PrevLogTerm int       // term of prevLogIndex entry
	LogEntry     []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)

	LeaderCommit int // leader’s commitIndex
}

func (rf *Raft) genAppendEntriesArgs(server int) *AppendEntriesArgs {
	args := &AppendEntriesArgs{
		Term:         rf.currentTermID,
		PrevLogIndex: rf.PrevLogIndex(server),
		PrevLogTerm:  rf.PrevLogTerm(server),
		LeaderCommit: rf.commitIndex,
	}

	args.LogEntry = make([]LogEntry, 0)
	args.LogEntry = append(args.LogEntry, rf.raftLog.EntriesInRange(args.PrevLogIndex+1, rf.raftLog.LastIndex()+1)...)

	return args
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	Confict bool
	XTerm   int // term in the conflicting entry (if any)
	XIndex  int // index of first entry with that term (if any)
	XLen    int // log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTermID
	reply.Success = false
	reply.Confict = false

	//  Reply false if term < currentTerm (§5.1)
	if rf.currentTermID > args.Term {
		return
	}

	if args.Term > rf.currentTermID {
		rf.convertToFollower(args.Term)
	} else if args.Term == rf.currentTermID && rf.state == Candidate {
		rf.convertToFollower(args.Term)
	}

	rf.electionTimerReset()

	if args.PrevLogIndex < rf.raftLog.FirstIndex() {
		rf.DPrintf("prev log index %d < first index %d\n", args.PrevLogIndex, rf.raftLog.FirstIndex())
		return
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > rf.raftLog.LastIndex() {
		reply.Confict = true
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.raftLog.LastIndex() + 1
		return
	}

	term := rf.raftLog.EntryAt(args.PrevLogIndex).Term
	if term != args.PrevLogTerm {
		reply.Confict = true
		reply.XTerm = term
		index := rf.raftLog.FirstIndex()
		for i := args.PrevLogIndex - 1 - rf.raftLog.FirstIndex(); i >= rf.raftLog.FirstIndex(); i-- {
			if rf.raftLog[i].Term != term {
				index = i + 1 + rf.raftLog.FirstIndex()
				break
			}
		}
		reply.XIndex = index
		reply.XLen = rf.raftLog.LastIndex() + 1
		return
	}

	start := args.PrevLogIndex + 1 - rf.raftLog.FirstIndex()
	for i, e := range args.LogEntry {
		if start+i >= len(rf.raftLog) || rf.raftLog[start+i].Term != e.Term {
			rf.raftLog = append(rf.raftLog[:start+i], args.LogEntry[i:]...)
			rf.persist()
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := rf.raftLog.LastIndex()
		newCommitIndex := min(args.LeaderCommit, lastNewEntryIndex)
		if rf.commitIndex < newCommitIndex {
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
			rf.DPrintf("update commit index to %d\n", rf.commitIndex)
		}
	}
	reply.Term, reply.Success = rf.currentTermID, true
}

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LastIncludedIndex int    // snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of LastIncludedIndex
	Data              []byte // raw bytes of snapshot chunk, starting at offset
}

func (rf *Raft) genInstallSnapshotArgs() *InstallSnapshotArgs {
	args := &InstallSnapshotArgs{
		Term:              rf.currentTermID,
		LastIncludedIndex: rf.raftLog.FirstIndex(),
		LastIncludedTerm:  rf.raftLog.FirstTerm(),
		Data:              rf.persister.ReadSnapshot(),
	}
	return args
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTermID
	// Reply immediately if term < currentTerm
	if args.Term < rf.currentTermID {
		return
	}

	if args.Term > rf.currentTermID {
		rf.convertToFollower(args.Term)
	} else if args.Term == rf.currentTermID && rf.state == Candidate {
		rf.convertToFollower(args.Term)
	}

	rf.electionTimerReset()

	if rf.commitIndex >= args.LastIncludedIndex {
		return
	}

	rf.RenewLog(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.persister.Save(rf.encodeState(), args.Data)

	rf.commitIndex = rf.raftLog.FirstIndex()
	rf.DPrintf("update commitIndex to %d\n", rf.commitIndex)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
