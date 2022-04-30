package shardkv

import (
	"6.824/labrpc"
	"bytes"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
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
	GET    string = "Get"
	PUT    string = "Put"
	APPEND string = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Key   string
	Value string

	//Dup elimination
	ClientId int64
	CmdId    int64
}

type OpResult struct {
	ClientId int64
	CmdId    int64
	Err      Err
	Value    string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	dead int32 // set by Kill()

	// Your definitions here.
	lastApplied int //last applied index, for log compaction

	kvStorage       map[string]string
	clientCmdIdMap  map[int64]int64
	finishedOpChans map[int]chan OpResult
}

func (kv *ShardKV) registerChanAtIdx(index int) chan OpResult {
	_, ok := kv.finishedOpChans[index]
	if !ok {
		kv.finishedOpChans[index] = make(chan OpResult, 1)
	}
	return kv.finishedOpChans[index]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		ClientId: args.ClientId,
		CmdId:    args.CmdId,
	}
	DPrintf("SERVER_GET: Server receives GET op with CmdId %v and key %v from client %v", args.CmdId, args.Key, args.ClientId)
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("SERVER_GET: ErrWrongLeader from START: GET op with CmdId %v and key %v from client %v", args.CmdId, args.Key, args.ClientId)
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	resChan := kv.registerChanAtIdx(index)
	kv.mu.Unlock()
	select {
	case opResult := <-resChan:
		if kv.isSameOp(&op, &opResult) && kv.rf.IsLeader() {
			kv.mu.Lock()
			if kv.maxraftstate > 0 && kv.maxraftstate < kv.rf.GetPersisterLogSize() {
				DPrintf("KVSERVER_SNAP: kv.maxraftstate: %v, kv.lastApplied : %v, rf.logsize: %v", kv.maxraftstate, kv.lastApplied, kv.rf.GetPersisterLogSize())
				kv.rf.Snapshot(index, kv.takeSnapshot())
			}
			kv.mu.Unlock()
			reply.Err = opResult.Err
			reply.Value = opResult.Value
		} else {
			DPrintf("SERVER_GET: ErrWrongLeader: GET op with CmdId %v and key %v from client %v", args.CmdId, args.Key, args.ClientId)
			reply.Err = ErrWrongLeader
		}
	case <-time.After(200 * time.Millisecond):
		DPrintf("SERVER_GET: Timeout, ErrWrongLeader: GET op with CmdId %v and key %v from client %v", args.CmdId, args.Key, args.ClientId)
		reply.Err = ErrWrongLeader
	}
	go kv.deleteChanAtIdx(index)
}

func (kv *ShardKV) deleteChanAtIdx(index int) {
	kv.mu.Lock()
	_, ok := kv.finishedOpChans[index]
	if ok {
		delete(kv.finishedOpChans, index)
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		CmdId:    args.CmdId,
	}
	DPrintf("SERVER_PUT_APPEND: Server receives %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("SERVER_PUT_APPEND: ErrWrongLeader from START: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("SERVER_PUT_APPEND: HAS_LEADER: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
	kv.mu.Lock()
	resChan := kv.registerChanAtIdx(index)
	kv.mu.Unlock()
	select {
	case opResult := <-resChan:
		if kv.isSameOp(&op, &opResult) && kv.rf.IsLeader() {
			DPrintf("SERVER_PUT_APPEND: OK: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
			kv.mu.Lock()
			if kv.maxraftstate > 0 && kv.maxraftstate < kv.rf.GetPersisterLogSize() {
				DPrintf("KVSERVER_SNAP: kv.maxraftstate: %v, kv.lastApplied : %v, rf.logsize: %v", kv.maxraftstate, kv.lastApplied, kv.rf.GetPersisterLogSize())
				kv.rf.Snapshot(index, kv.takeSnapshot())
			}
			kv.mu.Unlock()
			reply.Err = OK
		} else {
			DPrintf("SERVER_PUT_APPEND: ErrWrongLeader: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
			reply.Err = ErrWrongLeader
		}
	case <-time.After(200 * time.Millisecond):
		DPrintf("SERVER_PUT_APPEND: ErrWrongLeader from TIMEOUT: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
		reply.Err = ErrWrongLeader
	}
	go kv.deleteChanAtIdx(index)
}

func (kv *ShardKV) isSameOp(op *Op, opResult *OpResult) bool {
	return op.CmdId == opResult.CmdId && op.ClientId == opResult.ClientId
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvStorage = make(map[string]string)
	kv.clientCmdIdMap = make(map[int64]int64)
	kv.finishedOpChans = make(map[int]chan OpResult)

	kv.lastApplied = -1

	kv.readSnapshot(kv.rf.GetSnapshot())
	DPrintf("Initial kvStorage: %v", kv.kvStorage)

	go kv.applier()

	return kv
}

func (kv *ShardKV) checkAndApply(op *Op) OpResult {
	//lastCmdId, ok := kv.clientCmdIdMap[op.ClientId]
	opResult := OpResult{
		ClientId: op.ClientId,
		CmdId:    op.CmdId,
	}
	if kv.checkOpUpToDate(op) {
		kv.apply(op, &opResult)
		kv.clientCmdIdMap[op.ClientId] = op.CmdId
	} else if op.Type == GET {
		kv.apply(op, &opResult)
	}
	return opResult
}

func (kv *ShardKV) checkOpUpToDate(op *Op) bool {
	lastCmdId, ok := kv.clientCmdIdMap[op.ClientId]
	return !ok || lastCmdId < op.CmdId
}

func (kv *ShardKV) applier() {

	for !kv.killed() {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			index := applyMsg.CommandIndex
			kv.mu.Lock()
			op := Op{}
			opRes := OpResult{
				ClientId: 0,
				CmdId:    0,
				Err:      "",
				Value:    "",
			}
			if applyMsg.Command != nil {
				op = applyMsg.Command.(Op)
				opRes = kv.checkAndApply(&op)
			} else {
				continue
			}
			if opRes.Err == OK || opRes.Err == ErrNoKey {
				kv.lastApplied = index
			}
			opResChan, ok := kv.finishedOpChans[index]
			if !ok {
				opResChan = make(chan OpResult, 1)
				kv.finishedOpChans[index] = opResChan
			}
			if kv.maxraftstate > 0 && kv.maxraftstate < kv.rf.GetPersisterLogSize() {
				DPrintf("KVSERVER_SNAP: kv.maxraftstate: %v, kv.lastApplied : %v, rf.logsize: %v", kv.maxraftstate, kv.lastApplied, kv.rf.GetPersisterLogSize())
				kv.rf.Snapshot(index-1, kv.takeSnapshot())
			}
			kv.mu.Unlock()
			opResChan <- opRes
		} else if applyMsg.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
				kv.readSnapshot(applyMsg.Snapshot)
				kv.lastApplied = applyMsg.SnapshotIndex
			}
			kv.mu.Unlock()
		} else {
			DPrintf("KV.APPLIER: Unknown type of applyMsg")
		}
	}
}

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var newkvStorage map[string]string
	var newclientCmdIdMap map[int64]int64
	if d.Decode(&newkvStorage) != nil || d.Decode(&newclientCmdIdMap) != nil {
		DPrintf("READ_SNAP_SHOT: read persist went wrong!")
	} else {
		DPrintf("READ_SNAP_SHOT: read persist successful!")
		kv.kvStorage = newkvStorage
		kv.clientCmdIdMap = newclientCmdIdMap
	}
}

func (kv *ShardKV) takeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStorage)
	e.Encode(kv.clientCmdIdMap)
	return w.Bytes()
}

func (kv *ShardKV) apply(op *Op, opResult *OpResult) {
	if op.Type == GET {
		value, ok := kv.kvStorage[op.Key]
		if !ok {
			opResult.Value = ""
			opResult.Err = ErrNoKey
		} else {
			opResult.Value = value
			opResult.Err = OK
		}
	} else if op.Type == PUT {
		kv.kvStorage[op.Key] = op.Value
		opResult.Err = OK
	} else if op.Type == APPEND {
		kv.kvStorage[op.Key] += op.Value
		opResult.Err = OK
	} else {
		DPrintf("KV.APPLY: UNKNOWN OPERATION: %v", op.Type)
	}
}
