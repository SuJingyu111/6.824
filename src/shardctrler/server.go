package shardctrler

import (
	"6.824/raft"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

//Operation types
const (
	JOIN  string = "Join"
	LEAVE string = "Leave"
	MOVE  string = "Move"
	QUERY string = "Query"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	//Added by me
	dead int32 // set by Kill()

	// Your data here.
	clientCmdIdMap  map[int64]int64
	configs         []Config // indexed by config num
	finishedOpChans map[int]chan OpResult

	lastApplied int64
}

type Op struct {
	// Your data here.
	Type string

	Servers map[int][]string //JOIN

	GIDs []int //LEAVE

	Shard int //MOVE
	GID   int //MOVE

	Num int //QUERY

	//Dup elimination
	ClientId int64
	CmdId    int64
}

type OpResult struct {
	ClientId int64
	CmdId    int64
	Err      Err
	Config   Config //For query
}

func (sc *ShardCtrler) registerChanAtIdx(index int) chan OpResult {
	_, ok := sc.finishedOpChans[index]
	if !ok {
		sc.finishedOpChans[index] = make(chan OpResult, 1)
	}
	return sc.finishedOpChans[index]
}

func (sc *ShardCtrler) deleteChanAtIdx(index int) {
	sc.mu.Lock()
	_, ok := sc.finishedOpChans[index]
	if ok {
		delete(sc.finishedOpChans, index)
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) isSameOp(op *Op, opResult *OpResult) bool {
	return op.CmdId == opResult.CmdId && op.ClientId == opResult.ClientId
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Type:     JOIN,
		Servers:  args.Servers,
		ClientId: args.ClientId,
		CmdId:    args.CmdId,
	}
	//DPrintf("JOIN: Server receives %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		//DPrintf("SERVER_PUT_APPEND: ErrWrongLeader from START: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
		reply.WrongLeader = true
		return
	}
	//DPrintf("SERVER_PUT_APPEND: HAS_LEADER: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
	sc.mu.Lock()
	resChan := sc.registerChanAtIdx(index)
	sc.mu.Unlock()
	select {
	case opResult := <-resChan:
		if sc.isSameOp(&op, &opResult) && sc.rf.IsLeader() {
			//DPrintf("SERVER_PUT_APPEND: OK: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
			reply.Err = OK
		} else {
			//DPrintf("SERVER_PUT_APPEND: ErrWrongLeader: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
			reply.WrongLeader = true
		}
	case <-time.After(200 * time.Millisecond):
		//DPrintf("SERVER_PUT_APPEND: ErrWrongLeader from TIMEOUT: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
		reply.WrongLeader = true
	}
	go sc.deleteChanAtIdx(index)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Type:     LEAVE,
		GIDs:     args.GIDs,
		ClientId: args.ClientId,
		CmdId:    args.CmdId,
	}
	//DPrintf("JOIN: Server receives %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		//DPrintf("SERVER_PUT_APPEND: ErrWrongLeader from START: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
		reply.WrongLeader = true
		return
	}
	//DPrintf("SERVER_PUT_APPEND: HAS_LEADER: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
	sc.mu.Lock()
	resChan := sc.registerChanAtIdx(index)
	sc.mu.Unlock()
	select {
	case opResult := <-resChan:
		if sc.isSameOp(&op, &opResult) && sc.rf.IsLeader() {
			//DPrintf("SERVER_PUT_APPEND: OK: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
			reply.Err = OK
		} else {
			//DPrintf("SERVER_PUT_APPEND: ErrWrongLeader: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
			reply.WrongLeader = true
		}
	case <-time.After(200 * time.Millisecond):
		//DPrintf("SERVER_PUT_APPEND: ErrWrongLeader from TIMEOUT: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
		reply.WrongLeader = true
	}
	go sc.deleteChanAtIdx(index)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Type:     LEAVE,
		Shard:    args.Shard,
		GID:      args.GID,
		ClientId: args.ClientId,
		CmdId:    args.CmdId,
	}
	//DPrintf("JOIN: Server receives %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		//DPrintf("SERVER_PUT_APPEND: ErrWrongLeader from START: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
		reply.WrongLeader = true
		return
	}
	//DPrintf("SERVER_PUT_APPEND: HAS_LEADER: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
	sc.mu.Lock()
	resChan := sc.registerChanAtIdx(index)
	sc.mu.Unlock()
	select {
	case opResult := <-resChan:
		if sc.isSameOp(&op, &opResult) && sc.rf.IsLeader() {
			//DPrintf("SERVER_PUT_APPEND: OK: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
			reply.Err = OK
		} else {
			//DPrintf("SERVER_PUT_APPEND: ErrWrongLeader: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
			reply.WrongLeader = true
		}
	case <-time.After(200 * time.Millisecond):
		//DPrintf("SERVER_PUT_APPEND: ErrWrongLeader from TIMEOUT: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
		reply.WrongLeader = true
	}
	go sc.deleteChanAtIdx(index)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Type:     LEAVE,
		Num:      args.Num,
		ClientId: args.ClientId,
		CmdId:    args.CmdId,
	}
	//DPrintf("JOIN: Server receives %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		//DPrintf("SERVER_PUT_APPEND: ErrWrongLeader from START: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
		reply.WrongLeader = true
		return
	}
	//DPrintf("SERVER_PUT_APPEND: HAS_LEADER: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
	sc.mu.Lock()
	resChan := sc.registerChanAtIdx(index)
	sc.mu.Unlock()
	select {
	case opResult := <-resChan:
		if sc.isSameOp(&op, &opResult) && sc.rf.IsLeader() {
			//DPrintf("SERVER_PUT_APPEND: OK: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
			reply.Err = OK
			reply.Config = opResult.Config
		} else {
			//DPrintf("SERVER_PUT_APPEND: ErrWrongLeader: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
			reply.WrongLeader = true
		}
	case <-time.After(200 * time.Millisecond):
		//DPrintf("SERVER_PUT_APPEND: ErrWrongLeader from TIMEOUT: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
		reply.WrongLeader = true
	}
	go sc.deleteChanAtIdx(index)
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastApplied = -1
	sc.clientCmdIdMap = make(map[int64]int64)
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.finishedOpChans = make(map[int]chan OpResult)

	go sc.applier()

	return sc
}

func (sc *ShardCtrler) applier() {

	for !sc.killed() {
		applyMsg := <-sc.applyCh
		if applyMsg.CommandValid {
			index := applyMsg.CommandIndex
			sc.mu.Lock()
			op := Op{}
			opRes := OpResult{
				ClientId: 0,
				CmdId:    0,
				Err:      "",
			}
			if applyMsg.Command != nil {
				op = applyMsg.Command.(Op)
				opRes = sc.checkAndApply(&op)
			} else {
				continue
			}
			if opRes.Err == OK {
				sc.lastApplied = int64(index)
			}
			opResChan, ok := sc.finishedOpChans[index]
			if !ok {
				opResChan = make(chan OpResult, 1)
				sc.finishedOpChans[index] = opResChan
			}
			sc.mu.Unlock()
			opResChan <- opRes
		} else if applyMsg.SnapshotValid {
			DPrintf("USED SNAPSHOT")
		} else {
			DPrintf("SC.APPLIER: Unknown type of applyMsg")
		}
	}
}

func (sc *ShardCtrler) checkAndApply(op *Op) OpResult {
	//lastCmdId, ok := kv.clientCmdIdMap[op.ClientId]
	opResult := OpResult{
		ClientId: op.ClientId,
		CmdId:    op.CmdId,
	}
	if sc.checkOpUpToDate(op) {
		sc.apply(op, &opResult)
		sc.clientCmdIdMap[op.ClientId] = op.CmdId
	} else if op.Type == QUERY {
		sc.apply(op, &opResult)
	}
	return opResult
}

func (sc *ShardCtrler) checkOpUpToDate(op *Op) bool {
	lastCmdId, ok := sc.clientCmdIdMap[op.ClientId]
	return !ok || lastCmdId < op.CmdId
}

func (sc *ShardCtrler) apply(op *Op, opResult *OpResult) {
	if op.Type == QUERY {
		sc.mu.Lock()
		if op.Num == -1 || op.Num >= len(sc.configs) {
			opResult.Config = sc.configs[len(sc.configs)-1]
		} else {
			opResult.Config = sc.configs[op.Num]
		}
		sc.mu.Unlock()
		opResult.Err = OK
	} else if op.Type == JOIN {
		newConfig := Config{}
		sc.mu.Lock()
		lastConfig := sc.getLastConfig()
		newConfig.Num = len(sc.configs)
		newGroup := make(map[int][]string)
		lastGroup := lastConfig.Groups
		for key, value := range lastGroup {
			newGroup[key] = value
		}
		for gid, servers := range op.Servers {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
		newConfig.Groups = newGroup

		newConfig.Shards = lastConfig.Shards
		groupShardMap := sc.getGroupShardMap(newConfig.Shards)
		minGid, maxGid := sc.getMinGid(groupShardMap), sc.getMaxGid(groupShardMap)
		for len(groupShardMap[maxGid])-len(groupShardMap[minGid]) > 1 {
			groupShardMap[minGid] = append(groupShardMap[minGid], groupShardMap[minGid][0])
			groupShardMap[maxGid] = groupShardMap[maxGid][1:]
			minGid, maxGid = sc.getMinGid(groupShardMap), sc.getMaxGid(groupShardMap)
		}
		var newShards [NShards]int
		for gid, shards := range groupShardMap {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
		newConfig.Shards = newShards
		sc.configs = append(sc.configs, newConfig)
		sc.mu.Unlock()
		opResult.Err = OK
	} else if op.Type == LEAVE {
		newConfig := Config{}
		sc.mu.Lock()
		lastConfig := sc.getLastConfig()
		//new config Num
		newConfig.Num = len(sc.configs)

		newGroup := make(map[int][]string)
		lastGroup := lastConfig.Groups
		for key, value := range lastGroup {
			newGroup[key] = value
		}
		newConfig.Groups = newGroup

		newConfig.Shards = lastConfig.Shards
		groupShardMap := sc.getGroupShardMap(newConfig.Shards)
		freeShards := make([]int, 0)
		var newShards [NShards]int
		for _, gid := range op.GIDs {
			if shards, ok := groupShardMap[gid]; ok {
				freeShards = append(freeShards, shards...)
				delete(groupShardMap, gid)
			}
			if _, ok := newConfig.Groups[gid]; ok {
				delete(newConfig.Groups, gid)
			}
		}
		if len(newConfig.Groups) > 0 {
			for _, shard := range freeShards {
				gid := sc.getMinGid(groupShardMap)
				groupShardMap[gid] = append(groupShardMap[gid], shard)
			}
			for gid, shards := range groupShardMap {
				for _, shard := range shards {
					newShards[shard] = gid
				}
			}
		}
		newConfig.Shards = newShards
		sc.configs = append(sc.configs, newConfig)
		sc.mu.Unlock()
		opResult.Err = OK
	} else if op.Type == MOVE {
		newConfig := Config{}
		sc.mu.Lock()
		//num
		newConfig.Num = len(sc.configs)
		//shard update
		lastConfig := sc.getLastConfig()
		newConfig.Shards = lastConfig.Shards
		newConfig.Shards[op.Shard] = op.GID
		//copy group map
		newGroup := make(map[int][]string)
		lastGroup := lastConfig.Groups
		for k, v := range lastGroup {
			newGroup[k] = v
		}
		newConfig.Groups = newGroup
		sc.configs = append(sc.configs, newConfig)
		sc.mu.Unlock()
		opResult.Err = OK
	} else {
		DPrintf("SC.APPLY: UNKNOWN OPERATION: %v", op.Type)
	}
}

//Unlocked
func (sc *ShardCtrler) getLastConfig() Config {
	return sc.configs[len(sc.configs)-1]
}

func (sc *ShardCtrler) getGroupShardMap(shards [NShards]int) map[int][]int {
	groupShardMap := make(map[int][]int)
	for shard, gid := range shards {
		if _, ok := groupShardMap[gid]; !ok {
			groupShardMap[gid] = make([]int, 0)
		}
		groupShardMap[gid] = append(groupShardMap[gid], shard)
	}
	return groupShardMap
}

func (sc *ShardCtrler) getMinGid(groupShardMap map[int][]int) int {
	minGid := -1
	minLen := NShards + 1
	for gid, shards := range groupShardMap {
		if len(shards) < minLen {
			minGid = gid
			minLen = len(shards)
		}
	}
	return minGid
}

func (sc *ShardCtrler) getMaxGid(groupShardMap map[int][]int) int {
	maxGid := -1
	maxLen := -1
	for gid, shards := range groupShardMap {
		if len(shards) > maxLen {
			maxGid = gid
			maxLen = len(shards)
		}
	}
	return maxGid
}
