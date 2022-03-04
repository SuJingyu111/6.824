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
	done              bool
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
	DPrintf("COND_SNAP: {Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)

	// outdated snapshot
	if lastIncludedIndex <= rf.commitIndex {
		//DPrintf("{Node %v} rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}

	if lastIncludedIndex > rf.getLastLogIndex() {
		rf.log = make([]LogEtry, 1)
	} else {
		rf.trimLog(lastIncludedIndex)
		//rf.log = shrinkEntriesArray(rf.logs[lastIncludedIndex-rf.getFirstLog().Index:])
		//rf.logs[0].Command = nil
	}
	// update dummy entry with lastIncludedTerm and lastIncludedIndex
	//rf.logs[0].Term, rf.logs[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex

	rf.persister.SaveStateAndSnapshot(rf.serializeState(), snapshot)
	//DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), lastIncludedTerm, lastIncludedIndex)
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
	snapshotIndex := rf.lastLogIndexNotIncluded
	if index <= snapshotIndex {
		DPrintf("SNAP SHOT: {Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	rf.trimLog(index)
	rf.persister.SaveStateAndSnapshot(rf.serializeState(), snapshot)
	DPrintf("SNAP SHOT: {Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller", rf.me, rf.serverState, rf.currentTerm, rf.commitIndex, rf.lastApplied, index, snapshotIndex)
}

func (rf *Raft) trimLog(index int) {
	DPrintf("TRIM: Log content of server %v before trim up to index %v: %v", rf.me, index, rf.log)
	rf.lastLogIndexNotIncluded = index
	rf.lastLogTermNotIncluded = rf.log[index-rf.lastLogIndexNotIncluded-1].Term
	rf.log = append([]LogEtry{}, rf.log[index-rf.lastLogIndexNotIncluded:]...)
	DPrintf("TRIM: Log content of server %v after trim up to index %v: %v", rf.me, index, rf.log)
}

func (rf *Raft) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastLogIndexNotIncluded)
	e.Encode(rf.lastLogTermNotIncluded)
	return w.Bytes()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapShotArg, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("IN INSTALL_SNAP*************************")
	defer DPrintf("INSTALL_SNAP: {Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v} before processing InstallSnapshotRequest %v and reply InstallSnapshotResponse %v", rf.me, rf.serverState, rf.currentTerm, rf.commitIndex, rf.lastApplied, args, reply)

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.newTerm(args.Term)
		rf.persist()
	}
	rf.resetTimeAndTimeOut()

	// outdated snapshot
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapShotArg, reply *InstallSnapShotReply) bool {
	DPrintf("In SD_INSTALL_SNAP****************")
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			DPrintf("SD_INSTALL_SNAP: Server %v reply term %v greater than self term %v, turn follower", rf.me, reply.Term, rf.currentTerm)
			rf.newTerm(reply.Term)
		} else {
			DPrintf("SD_INSTALL_SNAP: Server %v install snapshot to server %v success", rf.me, server)
		}
	} else {
		DPrintf("SD_INSTALL_SNAP: Server %v install snapshot to server %v RPC fail", rf.me, server)
	}
	return true
}
