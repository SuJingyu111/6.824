package raft

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
}

func (rf *Raft) InstallSnapshot(args *InstallSnapShotArg, reply *InstallSnapShotReply) {

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapShotArg, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
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
