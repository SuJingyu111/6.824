package raft

import "math"

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
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
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			DPrintf("APPLIER: Server %v applied entry with real index %v", rf.me, entry.Index)
			rf.lastApplied = entry.Index
		}
		rf.mu.Lock()
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		//if last > rf.lastApplied {
		//	rf.lastApplied = last - 1
		//}
		DPrintf("Server %v applies entries %v-%v in term %v", rf.me, first, last, rf.currentTerm)
		rf.mu.Unlock()
	}
}
