package raft

type LogEntry struct {
	Command any
	Term    int
	Index   int
}
// type RaftLogEntry struct {
// 	Command     any // The client command
// 	TermNumber  int // Term when entry was received by leader
// 	LogIndex    int // Index in the log
// }


type RaftLog []LogEntry

func (log RaftLog) FirstIndex() int {
	idx := 0
	return log[idx].Index
}

func (log RaftLog) FirstTerm() int {
	idx := 0
	return log[idx].Term
}

func (log RaftLog) LastIndex() int {
	idx := len(log) - 1
	return log[idx].Index
}

func (log RaftLog) LastTerm() int {
	idx := len(log) - 1
	return log[idx].Term
}

func (log RaftLog) EntryAt(index int) LogEntry {
	idx := index - log.FirstIndex()
	return log[idx]
}

func (log RaftLog) EntriesInRange(start, end int) []LogEntry {
	startIdx := start - log.FirstIndex()
	endIdx := end - log.FirstIndex()
	return log[startIdx:endIdx]
}

func (log *RaftLog) Append(command any, term int) int {
	Idx := log.LastIndex() + 1
	*log = append(*log, LogEntry{Command: command, Term: term, Index: Idx})
	return Idx
}

func (log *RaftLog) Initialize() {
	*log = make([]LogEntry, 0)
	*log = append(*log, LogEntry{Command: nil, Term: 0, Index: 0})
}
