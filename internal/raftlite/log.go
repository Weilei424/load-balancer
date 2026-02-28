package raftlite

// lastIndex returns the Index of the last log entry, or 0 if empty.
func lastIndex(log []LogEntry) int {
	if len(log) == 0 {
		return 0
	}
	return log[len(log)-1].Index
}

// lastTerm returns the Term of the last log entry, or 0 if empty.
func lastTerm(log []LogEntry) int {
	if len(log) == 0 {
		return 0
	}
	return log[len(log)-1].Term
}

// logSlice returns all entries with Index strictly greater than after.
func logSlice(log []LogEntry, after int) []LogEntry {
	for i, e := range log {
		if e.Index > after {
			return log[i:]
		}
	}
	return nil
}

// truncateFrom removes all entries with Index >= fromIndex and returns the result.
func truncateFrom(log []LogEntry, fromIndex int) []LogEntry {
	for i, e := range log {
		if e.Index >= fromIndex {
			return log[:i]
		}
	}
	return log
}

// termAt returns the Term of the entry at the given index, or 0 if not found.
func termAt(log []LogEntry, index int) int {
	if index == 0 {
		return 0
	}
	for i := len(log) - 1; i >= 0; i-- {
		if log[i].Index == index {
			return log[i].Term
		}
	}
	return 0
}

// firstIndexOfTerm returns the smallest Index in the given term, or 1 if not found.
func firstIndexOfTerm(log []LogEntry, term int) int {
	for _, e := range log {
		if e.Term == term {
			return e.Index
		}
	}
	return 1
}

// lastIndexOfTerm returns the largest Index in the given term, or 0 if not found.
func lastIndexOfTerm(log []LogEntry, term int) int {
	for i := len(log) - 1; i >= 0; i-- {
		if log[i].Term == term {
			return log[i].Index
		}
	}
	return 0
}

// entryAtIndex returns a copy of the entry with the given index and true, or zero+false.
func entryAtIndex(log []LogEntry, index int) (LogEntry, bool) {
	for _, e := range log {
		if e.Index == index {
			return e, true
		}
	}
	return LogEntry{}, false
}
