package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStorage       map[string]string
	clientCmdIdMap  map[int64]int64
	finishedOpChans map[int]chan OpResult

	lastApplied int64
}

func (kv *KVServer) registerChanAtIdx(index int) chan OpResult {
	_, ok := kv.finishedOpChans[index]
	if !ok {
		kv.finishedOpChans[index] = make(chan OpResult, 1)
	}
	return kv.finishedOpChans[index]
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:     args.OpType,
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

func (kv *KVServer) deleteChanAtIdx(index int) {
	kv.mu.Lock()
	_, ok := kv.finishedOpChans[index]
	if ok {
		delete(kv.finishedOpChans, index)
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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

func (kv *KVServer) isSameOp(op *Op, opResult *OpResult) bool {
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
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStorage = make(map[string]string)
	kv.clientCmdIdMap = make(map[int64]int64)
	kv.finishedOpChans = make(map[int]chan OpResult)

	kv.lastApplied = -1

	kv.readSnapshot(kv.rf.GetSnapshot())
	DPrintf("Initial kvStorage: %v", kv.kvStorage)

	go kv.applier()

	return kv
}

func (kv *KVServer) checkAndApply(op *Op) OpResult {
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

func (kv *KVServer) checkOpUpToDate(op *Op) bool {
	lastCmdId, ok := kv.clientCmdIdMap[op.ClientId]
	return !ok || lastCmdId < op.CmdId
}

func (kv *KVServer) applier() {

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
				kv.lastApplied = int64(index)
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
				kv.lastApplied = int64(applyMsg.SnapshotIndex)
			}
			kv.mu.Unlock()
		} else {
			DPrintf("KV.APPLIER: Unknown type of applyMsg")
		}
	}
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
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

func (kv *KVServer) takeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStorage)
	e.Encode(kv.clientCmdIdMap)
	return w.Bytes()
}

func (kv *KVServer) apply(op *Op, opResult *OpResult) {
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
