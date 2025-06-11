package raft

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	go rf.SaveSnapshot(index, snapshot)
}

func (rf *Raft) SaveSnapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DPrintf("save snapshot to index %d\n", index)
	if rf.raftLog.FirstIndex() >= index {
		return
	}

	if rf.lastApplied < index {
		return
	}

	rf.Shrink(index)
	rf.persister.Save(rf.encodeState(), snapshot)
}
