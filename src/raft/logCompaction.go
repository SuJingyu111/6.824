package raft

import (
	"6.824/labgob"
	"bytes"
)

type InstallSnapShotArg struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapShotReply struct {
	Term int
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("COND_SNAP: Server %v  service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)

	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("COND_SNAP: Server %v rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}

	if lastIncludedIndex > rf.getLastLogIndex() {
		rf.log = make([]LogEtry, 0)
		DPrintf("")
	} else {
		rf.trimLog(lastIncludedIndex)
	}
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
	rf.lastLogIndexNotIncluded, rf.lastLogTermNotIncluded = lastIncludedIndex, lastIncludedTerm
	//rf.persist()

	rf.persister.SaveStateAndSnapshot(rf.serializeState(), snapshot)
	DPrintf("COND_SNAP: Server %v after install: lastApplied: %v, commitIndex: %v, log: %v", rf.me, rf.lastApplied, rf.commitIndex, rf.log)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("IN SNAPSHOT****************")
	if index <= rf.lastLogIndexNotIncluded {
		DPrintf("SNAP SHOT: Server %v rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, rf.lastLogIndexNotIncluded, rf.currentTerm)
		return
	}
	rf.trimLog(index)
	rf.persister.SaveStateAndSnapshot(rf.serializeState(), snapshot)
	DPrintf("SNAP SHOT: Server %v's state is {state %v,term %v,commitIndex %v,lastApplied %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller", rf.me, rf.serverState, rf.currentTerm, rf.commitIndex, rf.lastApplied, index, rf.lastLogIndexNotIncluded)
}

func (rf *Raft) trimLog(index int) {
	//TODO: LEAVE ONE DUMMY LOG AT THE HEAD TO WORK WITH THE FIRST LOG IN MAKE.
	DPrintf("TRIM: Log content of server %v before trim up to index %v: %v", rf.me, index, rf.log)
	DPrintf("TRIM: lastLogIndexNotIncluded: %v, index: %v, log length: %v", rf.lastLogTermNotIncluded, index, len(rf.log))
	DPrintf("TRIM: last real index: %v", index-rf.lastLogIndexNotIncluded-1)
	rf.lastLogTermNotIncluded = rf.log[index-rf.lastLogIndexNotIncluded-1].Term
	newLog := make([]LogEtry, rf.getLastLogIndex()-index)
	copy(newLog, rf.log[index-rf.lastLogIndexNotIncluded:])
	DPrintf("TRIM: new log: %v, supposed content: %v", newLog, rf.log[index-rf.lastLogIndexNotIncluded:])
	rf.log = newLog
	rf.lastLogIndexNotIncluded = index
	rf.persist()
	//rf.log = append([]LogEtry{}, rf.log[index-rf.lastLogIndexNotIncluded:]...)
	DPrintf("TRIM: Log content of server %v after trim up to index %v: %v", rf.me, index, rf.log)
}

func (rf *Raft) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	//e.Encode(rf.lastLogIndexNotIncluded)
	//e.Encode(rf.lastLogTermNotIncluded)
	return w.Bytes()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapShotArg, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("IN INSTALL_SNAP*************************")
	defer DPrintf("INSTALL_SNAP: {Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v} before processing InstallSnapshotRequest %v and reply InstallSnapshotResponse %v", rf.me, rf.serverState, rf.currentTerm, rf.commitIndex, rf.lastApplied, args, reply)

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.newTerm(args.Term)
		//rf.persist()
	}
	rf.resetTimeAndTimeOut()
	reply.Term = rf.currentTerm
	// outdated snapshot
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	//go func() {
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	//}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapShotArg, reply *InstallSnapShotReply) bool {
	DPrintf("In SD_INSTALL_SNAP: Send to server %v****************", server)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			DPrintf("SD_INSTALL_SNAP: Server %v reply term %v greater than self term %v, turn follower", rf.me, reply.Term, rf.currentTerm)
			rf.newTerm(reply.Term)
		} else {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
			DPrintf("SD_INSTALL_SNAP: Server %v install snapshot to server %v success, new next index: %v", rf.me, server, rf.nextIndex[server])
		}
	} else {
		DPrintf("SD_INSTALL_SNAP: Server %v install snapshot to server %v RPC fail", rf.me, server)
	}
	return true
}
