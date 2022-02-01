package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type WorkerArgs struct {
	FinishedMapIdx    int //The index of map task finished by this worker
	FinishedReduceIdx int //The index of reduce task finished by this worker
}

type CoordinatorReply struct {
	TaskType int //0 for map, 1 for reduce, 2 done

	NMap    int //Number of map tasks
	NReduce int //Number of reduce tasks

	MapIdx   int    //Index of map task, for map only
	FileName string // Name of file to read, for map only

	ReduceIdx int //Index of reduce task, for reduce only
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
