package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
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
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestAndReport(args *WorkerArgs, reply *CoordinatorReply) error {
	c.lock.Lock()
	if args.FinishedMapIdx >= 0 {
		_, ok := c.mapSet[args.FinishedMapIdx]
		if ok {
			delete(c.mapSet, args.FinishedMapIdx)
		}
		return nil
	}
	if args.FinishedReduceIdx >= 0 {
		_, ok := c.reduceSet[args.FinishedReduceIdx]
		if ok {
			delete(c.reduceSet, args.FinishedReduceIdx)
		}
		return nil
	}
	if len(c.reduceSet) == 0 {
		reply.TaskType = 3
		c.lock.Unlock()
	} else if len(c.mapSet) > 0 { // Allocate map task
		allocateIdx := -1
		//Find map tasks not yet allocated
		for key, val := range c.mapSet {
			if !val {
				allocateIdx = key
				break
			}
		}
		if allocateIdx == -1 {
			reply.TaskType = 2 //wait until all maps finish
			c.lock.Unlock()
		} else {
			reply.TaskType = 0 //map
			reply.NMap = c.nMap
			reply.NReduce = c.nReduce
			reply.MapIdx = allocateIdx
			reply.FileName = c.files[allocateIdx]
			c.mapSet[allocateIdx] = true
			c.lock.Unlock()
			//check if worker crashed
			go func() {
				time.Sleep(10 * time.Second)
				c.lock.Lock()
				_, ifContain := c.mapSet[allocateIdx]
				if ifContain {
					fmt.Printf("Coordinator: Assume worker crashed in map for %d.\n", allocateIdx)
					c.mapSet[allocateIdx] = false
				}
				c.lock.Unlock()
			}()
		}
	} else {
		allocateIdx := -1
		for key, val := range c.reduceSet {
			if !val {
				allocateIdx = key
				break
			}
		}
		if allocateIdx == -1 {
			reply.TaskType = 2
			fmt.Printf("Coordinator: worker wait for reduce to finish\n")
			c.lock.Unlock()
		} else {
			reply.TaskType = 1 //Reduce
			reply.NMap = c.nMap
			reply.NReduce = c.nReduce
			reply.ReduceIdx = allocateIdx
			fmt.Printf("Coordinator: Allocated reduce task %d.\n", allocateIdx)
			c.reduceSet[allocateIdx] = true
			c.lock.Unlock()
			go func() {
				time.Sleep(10 * time.Second)
				c.lock.Lock()
				_, ifContain := c.reduceSet[allocateIdx]
				if ifContain {
					fmt.Printf("Coordinator: Assume worker crashed in reduce.\n")
					c.reduceSet[allocateIdx] = false
				}
				c.lock.Unlock()
			}()
		}
	}
	return nil
}

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
