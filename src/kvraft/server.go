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
	kvStorage      map[string]string
	clientCmdIdMap map[int64]int64
	opResultMap    map[int]chan OpResult
}

func (kv *KVServer) registerChanAtIdx(index int) chan OpResult {
	_, ok := kv.opResultMap[index]
	if !ok {
		kv.opResultMap[index] = make(chan OpResult)
	}
	return kv.opResultMap[index]
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:     args.OpType,
		Key:      args.Key,
		ClientId: args.ClientId,
		CmdId:    args.CmdId,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	resChan := kv.registerChanAtIdx(index)
	kv.mu.Unlock()
	select {
	case opRes := <-resChan:
		if opRes.CmdId == args.CmdId && opRes.ClientId == args.ClientId {
			reply.Err = opRes.Err
			reply.Value = opRes.Value
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(200 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
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
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	resChan := kv.registerChanAtIdx(index)
	kv.mu.Unlock()
	select {
	case opRes := <-resChan:
		reply.Err = opRes.Err
		if opRes.CmdId == args.CmdId && opRes.ClientId == args.ClientId {
			reply.Err = opRes.Err
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(200 * time.Millisecond):
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
	kv.opResultMap = make(map[int]chan OpResult)

	go kv.applier()

	return kv
}

func (kv *KVServer) applier() {
	//TODO
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			index := applyMsg.CommandIndex
			op := applyMsg.Command.(Op)
			opResult := OpResult{
				ClientId: op.ClientId,
				CmdId:    op.CmdId,
			}
			kv.mu.Lock()
			if op.Type == GET {
				opResult = kv.apply(&op)
			} else {
				lastCmdId, ok := kv.clientCmdIdMap[op.ClientId]
				if !ok || lastCmdId < op.CmdId {
					opResult = kv.apply(&op)
					kv.clientCmdIdMap[op.ClientId] = op.CmdId
				}
			}
			opResChan, ok := kv.opResultMap[index]
			if !ok {
				opResChan = make(chan OpResult)
				kv.opResultMap[index] = opResChan
			}
			opResChan <- opResult
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) apply(op *Op) OpResult {
	opResult := OpResult{
		ClientId: op.ClientId,
		CmdId:    op.CmdId,
	}
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
	return opResult
}
