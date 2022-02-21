package raft

import "fmt"

//Argument for append entry rpcs
type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm int
	Entries     []LogEtry

	LeaderCommit int

	//IsHeartBeat bool
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//For heartbeat msg
	if args.Term >= rf.currentTerm {
		//If this server is a leader or candidate, then it should become follower
		if rf.serverState == LEADER {
			if args.Term > rf.currentTerm {
				rf.newTerm(args.Term)
			} else {
				_, err := DPrintf("Server %v and server %v are both leaders in Term %v", rf.me, args.LeaderId, args.Term)
				if err != nil {
					fmt.Printf("Problem in Dprintf")
				}
				//fmt.Printf("%v")
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
		//Send reply
		reply.Term = args.Term
		reply.Success = true
	}
}

func (rf *Raft) replicateAndCommitHandler() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() && rf.serverState == LEADER {
		//TODO: FILL IN THIS FUNCTION FOR 2B
		for server, nextIdx := range rf.nextIndex {
			if server == rf.me {
				continue
			}
			if nextIdx <= len(rf.log) {
				/*
					args := AppendEntryArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: nextIdx - 1,
						PrevLogTerm:  rf.log[nextIdx - 1].Term,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}
				*/
			}
		}
	}
}
