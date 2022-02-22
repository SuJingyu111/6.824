package raft

import (
	"fmt"
	"math"
	"time"
)

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
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf(" server %v Sent log staring from %v to server %v", rf.me, args.PrevLogIndex+1, server)
		if reply.Term > rf.currentTerm {
			rf.newTerm(reply.Term)
		} else if args.Term == rf.currentTerm {
			if reply.Success {
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			} else {
				rf.nextIndex[server] = int(math.Min(1.0, float64(rf.nextIndex[server]-1)))
			}
		}
	}
	return ok
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//TODO: MODIFY THIS
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
		//reply.Term = args.Term
		//reply.Success = true

		//Start Append
		if args.Entries != nil {
			if args.PrevLogIndex > len(rf.log) || (args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
				reply.Term = rf.currentTerm
				reply.Success = false
			} else {
				rf.log = rf.log[:args.PrevLogIndex]
				rf.log = append(rf.log, args.Entries...)
				reply.Term = rf.currentTerm
				reply.Success = true
			}
		} else {
			reply.Term = args.Term
			reply.Success = true
		}

		//Check commit index
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log))))
		}
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
	}
}

func (rf *Raft) replicateHandler() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		serverId := server
		go rf.replicate(serverId)
	}
}

func (rf *Raft) replicate(server int) {
	for {
		rf.mu.Lock()
		//defer rf.mu.Unlock()
		DPrintf("Replicate")
		if !rf.killed() && rf.serverState == LEADER {
			nextIdx := rf.nextIndex[server]
			if nextIdx <= len(rf.log) {
				args := AppendEntryArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: nextIdx - 1,
					//PrevLogTerm:  rf.log[nextIdx-1].Term,
					Entries:      rf.log[nextIdx-1:],
					LeaderCommit: rf.commitIndex,
				}
				if args.PrevLogIndex > 0 {
					args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
				}
				reply := AppendEntryReply{}
				serverId := server
				go rf.sendAppendEntry(serverId, &args, &reply)
			}

		} else {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (rf *Raft) commitHandler() {
	for {
		DPrintf("Replicate")
		rf.mu.Lock()
		newCommitIndex := rf.getMajorReplicatedIndex()
		if rf.log[newCommitIndex-1].Term == rf.currentTerm {
			rf.commitIndex = newCommitIndex
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) getMajorReplicatedIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	major := -1
	count := 0
	for _, matchIdx := range rf.matchIndex {
		if count == 0 {
			major = matchIdx
			count = 1
		} else if major == matchIdx {
			count += 1
		} else {
			count -= 1
		}
	}
	return major
}
