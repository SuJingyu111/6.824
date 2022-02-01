package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files   []string
	nMap    int
	nReduce int

	mapSet    map[int]bool //Collection of undone map tasks, if false, not allocated, if true, allocated
	reduceSet map[int]bool //Collection of undone reduce tasks, if false, not allocated, if true, allocated

	lock sync.Mutex

	taskChan chan CoordinatorReply
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := len(c.reduceSet) == 0
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapSet = make(map[int]bool)
	//fmt.Printf("nMap: %v, nReduce: %v \n", len(files), nReduce)
	c.lock = sync.Mutex{}
	c.taskChan = make(chan CoordinatorReply)
	for i := 0; i < len(files); i++ {
		c.mapSet[i] = false
	}
	c.reduceSet = make(map[int]bool)
	for i := 0; i < nReduce; i++ {
		c.reduceSet[i] = false
	}
	c.server()
	return &c
}
