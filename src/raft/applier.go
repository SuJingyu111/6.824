package raft

import "math"

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]LogEtry, commitIndex-lastApplied)
		first := int(math.Max(float64(0), float64(rf.lastApplied-rf.lastLogIndexNotIncluded)))
		last := int(math.Min(float64(len(rf.log)), float64(rf.commitIndex-rf.lastLogIndexNotIncluded)))
		DPrintf("APPLIER: Server %v, first: %v, last: %v", rf.me, first, last)
		if first < last {
			copy(entries, rf.log[first:last])
		}
		rf.lastApplied = lastApplied + len(entries)
		rf.mu.Unlock()
		for idx, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: lastApplied + idx + 1,
			}
			DPrintf("APPLIER: Server %v applied entry with real index %v and content %v", rf.me, lastApplied+idx+1, entry.Command)
		}
		/*
			rf.mu.Lock()

			DPrintf("Server %v applies entries %v-%v in term %v", rf.me, first, last, rf.currentTerm)
			rf.mu.Unlock()
		*/
	}
}
