package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
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

/*
type OpResult struct {
	ClientId int64
	CmdId    int64
	Err      Err
	Value    string
}

*/

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
	finishedOpChans map[int]chan Op
}

func (kv *KVServer) registerChanAtIdx(index int) chan Op {
	_, ok := kv.finishedOpChans[index]
	if !ok {
		kv.finishedOpChans[index] = make(chan Op, 1)
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
	case finishedOp := <-resChan:
		if op.CmdId == finishedOp.CmdId && op.ClientId == finishedOp.ClientId && kv.rf.IsLeader() {
			if finishedOp.Value == "" {
				reply.Err = ErrNoKey
				reply.Value = ""
				DPrintf("SERVER_GET: ErrNoKey: GET op with CmdId %v and key %v from client %v", args.CmdId, args.Key, args.ClientId)
			} else {
				DPrintf("SERVER_GET: OK: GET op with CmdId %v and key %v from client %v, value: %v", args.CmdId, args.Key, args.ClientId, finishedOp.Value)
				reply.Err = OK
				reply.Value = finishedOp.Value
			}
		} else {
			DPrintf("SERVER_GET: ErrWrongLeader: GET op with CmdId %v and key %v from client %v", args.CmdId, args.Key, args.ClientId)
			reply.Err = ErrWrongLeader
		}
	case <-time.After(200 * time.Millisecond):
		DPrintf("SERVER_GET: Timeout, ErrWrongLeader: GET op with CmdId %v and key %v from client %v", args.CmdId, args.Key, args.ClientId)
		reply.Err = ErrWrongLeader
	}
	/*
		kv.mu.Lock()
		kv.deleteChanAtIdx(index)
		kv.mu.Unlock()

	*/
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
	case finishedOp := <-resChan:
		if op.CmdId == finishedOp.CmdId && op.ClientId == finishedOp.ClientId && kv.rf.IsLeader() {
			DPrintf("SERVER_PUT_APPEND: OK: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
			reply.Err = OK
		} else {
			DPrintf("SERVER_PUT_APPEND: ErrWrongLeader: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
			reply.Err = ErrWrongLeader
		}
	case <-time.After(200 * time.Millisecond):
		DPrintf("SERVER_PUT_APPEND: ErrWrongLeader from TIMEOUT: %v op with CmdId %v and key %v, value %v, from client %v", args.Op, args.CmdId, args.Key, args.Value, args.ClientId)
		reply.Err = ErrWrongLeader
	}
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
	kv.finishedOpChans = make(map[int]chan Op)

	go kv.applier()

	return kv
}

func (kv *KVServer) checkAndApply(op *Op) {
	lastCmdId, ok := kv.clientCmdIdMap[op.ClientId]
	if !ok || lastCmdId < op.CmdId {
		kv.apply(op)
		kv.clientCmdIdMap[op.ClientId] = op.CmdId
	} else if op.Type == GET {
		kv.apply(op)
	}
}

func (kv *KVServer) applier() {

	for !kv.killed() {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			index := applyMsg.CommandIndex
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			kv.checkAndApply(&op)
			opResChan, ok := kv.finishedOpChans[index]
			if !ok {
				opResChan = make(chan Op, 1)
				kv.finishedOpChans[index] = opResChan
			}
			kv.mu.Unlock()
			opResChan <- op
		} else if applyMsg.SnapshotValid {
			//TODO: 3B
		}

	}
}

func (kv *KVServer) apply(op *Op) {
	if op.Type == GET {
		value, ok := kv.kvStorage[op.Key]
		if !ok {
			op.Value = ""
		} else {
			op.Value = value
		}
	} else if op.Type == PUT {
		kv.kvStorage[op.Key] = op.Value
	} else if op.Type == APPEND {
		kv.kvStorage[op.Key] += op.Value
	} else {
		DPrintf("KV.APPLY: UNKNOWN OPERATION: %v", op.Type)
	}
}
