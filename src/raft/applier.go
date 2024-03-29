package raft

import (
	"log"
	"math"
)

const ApplierDebug = false

func ApplierDPrintf(format string, a ...interface{}) (n int, err error) {
	if ApplierDebug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]LogEtry, commitIndex-lastApplied)
		first := int(math.Max(float64(0), float64(rf.getLogIdxOfLogicalIdx(rf.lastApplied+1))))
		last := int(math.Min(float64(len(rf.log)), float64(rf.getLogIdxOfLogicalIdx(rf.commitIndex+1))))
		DPrintf("APPLIER: Server %v, first: %v, last: %v", rf.me, first, last)
		if first < last {
			copy(entries, rf.log[first:last])
		}
		rf.lastApplied = lastApplied + len(entries)
		rf.mu.Unlock()
		//rf.appluMu.Lock()
		for idx, entry := range entries {
			if entry.Command == nil {
				ApplierDPrintf("RAFT_APPLIER: Command is nil")
			}
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: lastApplied + idx + 1,
			}
			//rf.mu.Lock()
			//rf.lastApplied = lastApplied + idx + 1
			//rf.mu.Unlock()
			//DPrintf("APPLIER: Server %v applied entry with real index %v and content %v", rf.me, lastApplied+idx+1, entry.Command)
		}
		//rf.appluMu.Unlock()
		/*
			rf.mu.Lock()

			DPrintf("Server %v applies entries %v-%v in term %v", rf.me, first, last, rf.currentTerm)
			rf.mu.Unlock()
		*/
	}
}
