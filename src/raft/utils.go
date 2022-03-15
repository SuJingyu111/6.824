package raft

import "log"

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

//Translate from absolute idx to idx in log
func (rf *Raft) getLogIdxOfLogicalIdx(logicalIdx int) int {
	if logicalIdx <= rf.lastLogIndexNotIncluded {
		log.Panic("getLogIdxOfLogicalIdx: logicalIdx <= rf.lastLogIndexNotIncluded")
	}
	return logicalIdx - rf.lastLogIndexNotIncluded - 1
}
