package raft

import (
	"fmt"
	"math"
	"sort"
)

//Argument for append entry rpcs
type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm int
	Entries     []LogEtry

	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Entries != nil {
		DPrintf("SD_APP_ENTRY: Leader %v in term %v sent log starting from %v to %v to server %v",
			rf.me, rf.currentTerm, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries), server)
	}
	if ok {
		if reply.Term > rf.currentTerm {
			rf.newTerm(reply.Term)
		} else if args.Term == rf.currentTerm {
			if reply.Success {
				DPrintf("SD_APP_ENTRY: server %v append success", server)
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				DPrintf("SD_APP_ENTRY: Next idx for server %v: %v, matchIdx: %v", server, rf.nextIndex[server], rf.matchIndex[server])
			} else {
				DPrintf("SD_APP_ENTRY: server %v append fail", server)
				rf.nextIndex[server] = args.PrevLogIndex
				nextTerm := rf.lastLogTermNotIncluded
				if rf.nextIndex[server] > rf.lastLogIndexNotIncluded {
					nextTerm = rf.log[rf.nextIndex[server]-rf.lastLogIndexNotIncluded-1].Term
				}
				DPrintf("SD_APP_ENTRY: nextIdx of server %v: %v, term of next idx: %v", server, rf.nextIndex[server], nextTerm)
				for rf.nextIndex[server] > rf.lastLogIndexNotIncluded && rf.log[rf.nextIndex[server]-rf.lastLogIndexNotIncluded-1].Term == nextTerm {
					rf.nextIndex[server] = rf.nextIndex[server] - 1
				}
				DPrintf("SD_APP_ENTRY: nextIdx of server %v: %v", server, rf.nextIndex[server])
			}
		}
	}
	return ok
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		//If this server is a leader or candidate, then it should become follower
		if rf.serverState == LEADER {
			if args.Term > rf.currentTerm {
				rf.newTerm(args.Term)
				DPrintf("APP_ENTRY: Leader %v receives app_ent rpc of higher term, turns follower", rf.me)
			} else {
				_, err := DPrintf("Server %v and server %v are both leaders in Term %v", rf.me, args.LeaderId, args.Term)
				if err != nil {
					fmt.Printf("Problem in Dprintf")
				}
			}
		} else if rf.serverState == CANDIDATE {
			rf.serverState = FOLLOWER
		}
		//If heartbeat Term is larger than current Term, update Term
		if args.Term > rf.currentTerm {
			rf.newTerm(args.Term)
		}
		//Reset election timeout, leader is alive
		rf.resetTimeAndTimeOut()

		//Start Append
		DPrintf("APP_ENTRY: prevLogIdx: %v, prevLogTerm: %v", args.PrevLogIndex, args.PrevLogTerm)
		if args.PrevLogIndex < rf.getLastLogIndex()+1 {
			//DPrintf("APP_ENTRY: Server %v pervLogTerm: %v", rf.me, rf.log[args.PrevLogIndex].Term)
		}
		//TODO: MODIFY HERE
		if args.PrevLogIndex >= rf.getLastLogIndex()+1 || (args.PrevLogIndex == rf.lastLogIndexNotIncluded && args.PrevLogTerm != rf.lastLogTermNotIncluded) ||
			(args.PrevLogIndex > rf.lastLogIndexNotIncluded && rf.log[args.PrevLogIndex-rf.lastLogIndexNotIncluded-1].Term != args.PrevLogTerm) {
			DPrintf("APP_ENTRY: Server %v refused log append from leader %v", rf.me, args.LeaderId)
			DPrintf("APP_ENTRY: For Server %v, parameters: args.PrevLogIndex: %v, rf.getLastLogIndex()+1: %v, rf.lastLogIndexNotIncluded: %v, "+
				"rf.lastLogTermNotIncluded: %v", rf.me, args.PrevLogIndex, rf.getLastLogIndex()+1, rf.lastLogIndexNotIncluded, rf.lastLogTermNotIncluded)
			DPrintf("APP_ENTRY: Server %v log content: %v", rf.me, rf.log)
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		} else {
			DPrintf("APP_ENTRY: args.PrevLogIndex %v, rf.lastLogIndexNotIncluded %v, rf.getFirstLogIndex() %v", args.PrevLogIndex, rf.lastLogIndexNotIncluded, rf.getFirstLogIndex())
			DPrintf("APP_ENTRY: Server %v ,log content before append: %v", rf.me, rf.log)
			if args.PrevLogIndex > rf.lastLogIndexNotIncluded {
				rf.log = rf.log[:args.PrevLogIndex+1-rf.lastLogIndexNotIncluded-1]
				rf.log = append(rf.log, args.Entries...)
			} else if args.PrevLogIndex == rf.lastLogIndexNotIncluded {
				rf.log = args.Entries
			} else {
				if len(rf.log) == 0 {
					firstLogIndex := rf.getFirstLogIndex() + 1
					rf.log = make([]LogEtry, 0)
					rf.log = append(rf.log, args.Entries[firstLogIndex-args.PrevLogIndex-1:]...)
				} else {
					firstLogIndex := rf.getFirstLogIndex()
					rf.log = make([]LogEtry, 0)
					rf.log = append(rf.log, args.Entries[firstLogIndex-args.PrevLogIndex-1:]...)
				}
			}
			rf.persist()
			DPrintf("APP_ENTRY: Server %v append entries, current log length: %v", rf.me, rf.getLastLogIndex()+1)
			DPrintf("APP_ENTRY: Server %v new log after append: %v", rf.me, rf.log)
			reply.Term = rf.currentTerm
			reply.Success = true
		}

		//Check commit index & apply
		if rf.commitIndex < args.LeaderCommit {
			DPrintf("IN APP ENTRY***")
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.getLastLogIndex())))
			rf.applyCond.Broadcast()
		} else {
			DPrintf("Server %v cannot commit", rf.me)
		}
	} else {
		DPrintf("APP_ENTRY: Server %v in term %v received append log of term %v and refused---", rf.me, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
	}
}

func (rf *Raft) commitHandler() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	DPrintf("IN COMMIT HANDLER***")
	newCommitIndex := rf.getMajorReplicatedIndex()
	prevCommitIndex := rf.commitIndex
	if newCommitIndex > rf.lastLogIndexNotIncluded && newCommitIndex < rf.getLastLogIndex()+1 && rf.log[newCommitIndex-rf.lastLogIndexNotIncluded-1].Term == rf.currentTerm && newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		rf.applyCond.Broadcast()
		//rf.sendHeartBeat()
	} else {
		DPrintf("Leader not commit, prevCommitIdx: %v, newCommitIdx: %v", prevCommitIndex, newCommitIndex)
	}
	DPrintf("After commit, leader %v commitIndex: %v", rf.me, rf.commitIndex)
}

func (rf *Raft) getMajorReplicatedIndex() int {
	logCpy := make([]int, len(rf.matchIndex))
	copy(logCpy, rf.matchIndex)
	sort.Ints(logCpy)
	return logCpy[len(logCpy)/2]
}

func (rf *Raft) applyInit() {
	DPrintf("APPLY_INIT: Server %v", rf.me)
	applyMsg := ApplyMsg{
		CommandValid: true,
		Command:      0,
		CommandIndex: 0,
	}
	rf.applyCh <- applyMsg
}
