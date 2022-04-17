package kvraft

import (
	"6.824/labrpc"
	"sync"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId          int64
	lastACKedLeaderId int64
	cmdId             int64

	mu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.cmdId = 0
	ck.lastACKedLeaderId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	thisCmdId := ck.cmdId
	ck.cmdId += 1
	thisClientId := ck.clientId
	thisLeaderId := ck.lastACKedLeaderId
	ck.mu.Unlock()

	args := GetArgs{
		Key:      key,
		ClientId: thisClientId,
		CmdId:    thisCmdId,
		OpType:   GET,
	}

	for {
		reply := GetReply{}
		//DPrintf("CLIENT_GET: Client %v sent get operation with ID %v to server %v", thisClientId, thisCmdId, thisLeaderId)
		ok := ck.servers[thisLeaderId].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == OK {
				atomic.StoreInt64(&ck.lastACKedLeaderId, thisLeaderId)
				//DPrintf("CLIENT_GET: OK: Client %v get operation with ID %v to server %v, value: %v", thisClientId, thisCmdId, thisLeaderId, reply.Value)
				return reply.Value
			} else if reply.Err == ErrNoKey {
				atomic.StoreInt64(&ck.lastACKedLeaderId, thisLeaderId)
				//DPrintf("CLIENT_GET: ErrNoKey: Client %v get operation with ID %v to server %v", thisClientId, thisCmdId, thisLeaderId)
				return ""
			} else {
				//DPrintf("CLIENT_GET: ErrWrongLeader: Client %v get operation with ID %v to server %v", thisClientId, thisCmdId, thisLeaderId)
				thisLeaderId = (thisLeaderId + 1) % int64(len(ck.servers))
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	// You will have to modify this function.
	ck.mu.Lock()
	thisCmdId := ck.cmdId
	ck.cmdId += 1
	thisClientId := ck.clientId
	thisLeaderId := ck.lastACKedLeaderId
	ck.mu.Unlock()

	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: thisClientId,
		CmdId:    thisCmdId,
	}

	for {
		reply := PutAppendReply{}
		//DPrintf("CLIENT_PUT_APPEND: client %v %v operation, cmdId: %v, leaderId: %v, key: %v, "+
		//	"value: %v", thisClientId, op, thisCmdId, thisLeaderId, key, value)
		ok := ck.servers[thisLeaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok && !(reply.Err == ErrWrongLeader) {
			atomic.StoreInt64(&ck.lastACKedLeaderId, thisLeaderId)
			//DPrintf("CLIENT_PUT_APPEND: OK: client %v %v operation, cmdId: %v, leaderId: %v, key: %v, "+
			//	"value: %v", thisClientId, op, thisCmdId, thisLeaderId, key, value)
			break
		} else {
			//DPrintf("CLIENT_PUT_APPEND: ErrWrongLeader: client %v %v operation, cmdId: %v, leaderId: %v, key: %v, "+
			//	"value: %v", thisClientId, op, thisCmdId, thisLeaderId, key, value)
			thisLeaderId = (thisLeaderId + 1) % int64(len(ck.servers))
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
