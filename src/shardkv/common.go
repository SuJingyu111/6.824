package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	CmdId     int64
	ConfigIdx int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	CmdId     int64
	Op        string
	ConfigIdx int
}

type GetReply struct {
	Err   Err
	Value string
}

type Shard struct {
	index          int
	kvStorage      map[string]string
	clientCmdIdMap map[int64]int64
}

func (shard *Shard) updateClientCmdIdMap(clientId int64, cmdId int64) {
	shard.clientCmdIdMap[clientId] = cmdId
}
