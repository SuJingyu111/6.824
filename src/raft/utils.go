package raft

//This is not locked! Check lock in outer scope!
func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return rf.lastLogIndexNotIncluded
	}
	return rf.lastLogIndexNotIncluded + len(rf.log)
}

//This is not locked! Check lock in outer scope!
func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastLogTermNotIncluded
	}
	return rf.log[len(rf.log)-1].Term
}

//This is not locked! Check lock in outer scope!
func (rf *Raft) getFirstLogIndex() int {
	if len(rf.log) == 0 {
		return rf.lastLogIndexNotIncluded
	}
	return rf.lastLogIndexNotIncluded + 1
}

//This is not locked! Check lock in outer scope!
func (rf *Raft) getFirstLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastLogTermNotIncluded
	}
	return rf.log[0].Term
}
