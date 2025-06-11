package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCond  *sync.Cond
	appendCond []*sync.Cond
	applyCh chan raftapi.ApplyMsg
	// Persistent state on all servers
	currentTermID int
	votedFor    int
	raftLog         RaftLog
	// Volatile state on all servers
	commitIndex int
	lastApplied int
	// Volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server
	state          ServerState
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	shutdownCh chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTermID
	isleader := rf.state == Leader
	return term, isleader
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command any) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state == Leader
	if !isLeader {
		return -1, -1, false
	}

	if command == nil {
		panic("command is nil")
	}
	index := rf.raftLog.Append(command, rf.currentTermID)
	rf.persist()
	rf.WakeAllAppender()

	return index, rf.currentTermID, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	close(rf.shutdownCh)
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != Leader {
				rf.becomeCandidate()
			}
			rf.resetElectionTimer()
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.broadcastHeartBeat()
			}
			rf.heartbeatTimerReset()
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {
	// Your initialization code here (3A, 3B, 3C).
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyCh:        applyCh,
		currentTermID:    0,
		votedFor:       -1,
		state:          Follower,
		commitIndex:    0,
		electionTimer:  time.NewTimer(time.Duration(50+rand.Int63()%300) * time.Millisecond),
		lastApplied:    0,
		heartbeatTimer: time.NewTimer(50 * time.Millisecond),
		shutdownCh:           make(chan int),
	}

	rf.raftLog.Initialize()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.nextIndex = fillSlice(len(peers), rf.raftLog.LastIndex()+1)
	rf.matchIndex = fillSlice(len(peers), 0)

	rf.applyCond = sync.NewCond(&rf.mu)
	rf.appendCond = make([]*sync.Cond, len(peers))
	for server := range peers {
		if server != me {
			rf.appendCond[server] = sync.NewCond(&sync.Mutex{})
			go rf.appender(server)
		}
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
