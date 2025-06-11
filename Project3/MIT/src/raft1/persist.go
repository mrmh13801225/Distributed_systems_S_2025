package raft

import (
	"bytes"
	"6.5840/labgob"
)

func (rf *Raft) encodeState() []byte {
	var buffer bytes.Buffer
	encoder := labgob.NewEncoder(&buffer)

	if err := encoder.Encode(rf.currentTermID); err != nil {
		panic("failed to encode currentTerm")
	}
	if err := encoder.Encode(rf.votedFor); err != nil {
		panic("failed to encode votedFor")
	}
	if err := encoder.Encode(rf.raftLog); err != nil {
		panic("failed to encode log")
	}

	return buffer.Bytes()
}

// persist saves Raft's persistent state to stable storage.
// The snapshot parameter is retrieved separately and saved alongside the state.
func (rf *Raft) persist() {
	state := rf.encodeState()
	snapshot := rf.persister.ReadSnapshot()
	rf.persister.Save(state, snapshot)
}

// readPersist restores the previously persisted state.
// This should be called during initialization.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if data == nil || len(data) == 0 {
		return // no persisted state
	}

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)

	var term int
	var votedFor int
	var log []LogEntry

	if err := decoder.Decode(&term); err != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&log) != nil {
		panic("readPersist: failed to decode state")
	}

	rf.currentTermID = term
	rf.votedFor = votedFor
	rf.raftLog = append([]LogEntry(nil), log...) // deep copy
}

// PersistBytes returns the size in bytes of the persisted Raft state.
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}
