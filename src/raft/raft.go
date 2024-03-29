package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	//"math"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// An entry in the log
type LogEtry struct {
	Term    int
	Command interface{}
	//Index   int
}

//Constants
const (
	CANDIDATE int = 1
	LEADER    int = 2
	FOLLOWER  int = 0
)

const HaveNotVoted int = -1

const (
	ElectionTimeBase         int = 100
	ElectionTimeRandInterval int = 100
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	appluMu   sync.Mutex

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent states
	currentTerm int
	votedFor    int
	log         []LogEtry

	//Volatile states on all servers
	commitIndex     int
	lastApplied     int
	applyCh         chan ApplyMsg
	applyCond       *sync.Cond
	serverState     int
	timeStamp       time.Time
	electionTimeOut time.Duration

	//Volatile states on leaders
	nextIndex  []int
	matchIndex []int

	//2D last log info.
	lastLogIndexNotIncluded int
	lastLogTermNotIncluded  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.serverState == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	data := rf.serializeState()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voterdFor int
	var persistLog []LogEtry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voterdFor) != nil || d.Decode(&persistLog) != nil { //|| d.Decode(&lastLogIndexNotIncluded) != nil || d.Decode(&lastLogTermNotIncluded) != nil
		DPrintf("read persist went wrong!")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voterdFor
		rf.log = persistLog
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		DPrintf("RQVOTE: Server %v receives higher term rqvote RPC. Turns follower from term %v to term %v", rf.me, rf.currentTerm, args.Term)
		rf.newTerm(args.Term)
	}

	lastLogTerm := rf.getLastLogTerm()
	lastLogIndex := rf.getLastLogIndex()

	candidateEligible := (lastLogTerm < args.LastLogTerm) ||
		(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex)
	DPrintf("RQVOTE: server %v receive rqvote rpc from server %v in term %v, eligible: %v",
		rf.me, args.CandidateId, args.Term, candidateEligible)

	if rf.currentTerm <= args.Term && candidateEligible && (rf.votedFor == HaveNotVoted || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		DPrintf("RQVOTE: server %v voted for server %v in term %v",
			rf.me, args.CandidateId, args.Term)
		rf.resetTimeAndTimeOut()
	} else {
		reply.VoteGranted = false
		DPrintf("RQVOTE: server %v dod not vote for server %v in term %v",
			rf.me, args.CandidateId, args.Term)
	}
	reply.Term = rf.currentTerm
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, vote *int) bool {
	//DPrintf("SD_REQ_VOTE: Server %v Sent vote request to server %v", server)
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		DPrintf("SD_RQ_VOTE: candidate %v sent vote request to server %v", rf.me, server)
		if reply.Term > rf.currentTerm {
			rf.newTerm(reply.Term)
			DPrintf("SD_RQ_VOTE: candidate %v receives higher term reply from to server %v, turns follower", rf.me, server)
		}

		if reply.Term == rf.currentTerm && reply.VoteGranted {
			*vote++
			if *vote > len(rf.peers)/2 && rf.currentTerm == args.Term && rf.serverState == CANDIDATE {
				DPrintf("SD_RQ_VOTE: candidate %v is now leader, total log length: %v-----------------------", rf.me, rf.getLastLogIndex()+1)
				DPrintf("SD_RQ_VOTE: candidate %v is now leader, log content: %v-----------------------", rf.me, rf.log)
				rf.elected()
				rf.sendHeartBeat()
			}
		}
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer rf.persist()
	index := -1
	term := -1
	isLeader := rf.serverState == LEADER
	// Your code here (2B).
	if !isLeader || rf.killed() {
		return index, term, isLeader
	}
	//Append log to own log list
	index = rf.getLastLogIndex() + 1
	term = rf.currentTerm
	newEntry := LogEtry{
		Term:    term,
		Command: command,
	}
	rf.log = append(rf.log, newEntry)
	rf.persist()
	rf.matchIndex[rf.me] = index
	DPrintf("START: Leader %v received command from client, total log length: %v", rf.me, index+1)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) IsLeader() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.serverState == LEADER
}

func (rf *Raft) GetPersisterLogSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetSnapshot() []byte {
	return rf.persister.ReadSnapshot()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.tick()
		sleepTime := rand.Intn(12) + 10
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
}

//Kicks off election/reset election time out
func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.serverState == LEADER {
		DPrintf("TICK : Leader %v tick", rf.me)
		rf.resetTimeAndTimeOut()
		rf.commitHandler()
		rf.sendHeartBeat()
	} else if time.Now().After(rf.timeStamp.Add(rf.electionTimeOut)) {
		DPrintf("TICK : Server %v start election", rf.me)
		rf.resetTimeAndTimeOut()
		rf.startElection()
	} else {
		DPrintf("TICK : Server %v tick", rf.me)
	}
}

//Reset the time stamp and election time out
func (rf *Raft) resetTimeAndTimeOut() {
	rf.timeStamp = time.Now()
	rf.electionTimeOut = time.Millisecond * time.Duration(ElectionTimeBase+rand.Intn(ElectionTimeRandInterval))
}

//Send heartBeat
func (rf *Raft) sendHeartBeat() {
	//rf.electionCond.L.Unlock()
	for i := range rf.peers {
		if !(rf.serverState == LEADER) || rf.killed() {
			break
		}
		server := i
		if server != rf.me {
			nextIdx := rf.nextIndex[server]
			entries := make([]LogEtry, rf.getLastLogIndex()-nextIdx+1)
			if nextIdx <= rf.lastLogIndexNotIncluded {
				DPrintf("!!!!!!!!!!!!!!!!!!!!!!!!!1")
				DPrintf("Leader %v nextIdx: %v, notIncluded: %v for server %v", rf.me, nextIdx, rf.lastLogIndexNotIncluded, server)
				args := InstallSnapShotArg{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastLogIndexNotIncluded,
					LastIncludedTerm:  rf.lastLogTermNotIncluded,
					Offset:            0,
					Data:              rf.persister.ReadSnapshot(),
					Done:              false,
				}
				reply := InstallSnapShotReply{}
				go rf.sendInstallSnapshot(server, &args, &reply)
			} else {
				startIdx := rf.getLogIdxOfLogicalIdx(nextIdx)
				DPrintf("SD_HEART_BEAT: start Idx: %v", startIdx)
				copy(entries, rf.log[startIdx:])
				prevLogTerm := rf.lastLogTermNotIncluded
				if startIdx > 0 {
					prevLogTerm = rf.log[startIdx-1].Term
				}
				args := AppendEntryArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: nextIdx - 1,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntryReply{
					Success: false,
				}
				go rf.sendAppendEntry(server, &args, &reply)
			}
		}
	}
}

//Kick off election process
func (rf *Raft) startElection() {
	rf.currentTerm += 1
	rf.serverState = CANDIDATE
	DPrintf("ELECTION: server %v start election on new term %v", rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	rf.persist()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	vote := 1

	for peer := range rf.peers {
		server := peer
		if server != rf.me {
			go rf.sendRequestVote(server, &args, &vote)
		}
	}
}

//Start of a new Term
func (rf *Raft) newTerm(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedFor = HaveNotVoted
	rf.serverState = FOLLOWER
	rf.persist()
}

func (rf *Raft) elected() {
	rf.serverState = LEADER
	lastLogIndex := rf.getLastLogIndex()
	DPrintf("ELECTED: Leader % v Elected===========", rf.me)
	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = lastLogIndex + 1
	}
	DPrintf("ELECTED: Leader %v nextIdx array %v===========", rf.me, rf.nextIndex)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	//rf.mu = new(sync.Mutex)

	// Your initialization code here (2A, 2B, 2C).
	//Persistent states
	rf.currentTerm = 0
	rf.votedFor = HaveNotVoted
	rf.log = make([]LogEtry, 1)
	rf.log[0] = LogEtry{Command: 0, Term: 0}

	//Volatile states on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.serverState = FOLLOWER
	rf.timeStamp = time.Now()
	rf.electionTimeOut = time.Millisecond * time.Duration(ElectionTimeBase+rand.Intn(ElectionTimeRandInterval))

	//Volatile states on leaders
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	//2D last log info. Persisted for recovery
	rf.lastLogIndexNotIncluded = -1
	rf.lastLogTermNotIncluded = -1

	//rf.applyInit()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//rf.apply(0, -1)
	go rf.applier()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
