package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Worker
// main/mrworker.go calls this function.
//

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	wArgs := WorkerArgs{
		FinishedMapIdx:    -1,
		FinishedReduceIdx: -1,
	}

	//Call coordinator handler
	for {
		cReply := CoordinatorReply{}
		coordRunning := call("Coordinator.RequestAndReport", &wArgs, &cReply)
		if !coordRunning || cReply.TaskType == 3 {
			break
		}
		if cReply.TaskType != 2 {
			if cReply.TaskType == 0 {
				//Do map
				//fmt.Printf("Get map task %d.\n", cReply.MapIdx)
				buckets := make([][]KeyValue, cReply.NReduce)
				filename := cReply.FileName
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))

				//Put intermediate kv into corresponding bucket
				for _, kv := range kva {
					reduceIdx := ihash(kv.Key) % cReply.NReduce
					buckets[reduceIdx] = append(buckets[reduceIdx], kv)
				}

				//Write each bucket into intermediate file
				for reduceIdx, bucket := range buckets {
					tempFileName := "mr-" + strconv.Itoa(cReply.MapIdx) + "-" + strconv.Itoa(reduceIdx)
					tempFile, err := ioutil.TempFile("", tempFileName)
					if err != nil {
						log.Fatalf("cannot create %v", tempFileName)
					}
					enc := json.NewEncoder(tempFile)
					for _, kv := range bucket {
						err := enc.Encode(&kv)
						if err != nil {
							log.Fatalf("cannot write into %v", tempFileName)
						}
					}
					oldName := tempFile.Name()
					//os.Rename(oldName, tempFileName)
					err = os.Rename(oldName, tempFileName)
					if err != nil {
						log.Fatalf("cannot rename %v to %v", oldName, tempFileName)
					}

					err = tempFile.Close()
					if err != nil {
						log.Fatalf("cannot close %v", tempFileName)
					}
				}
				//Report finished map task
				wArgs.FinishedMapIdx = cReply.MapIdx
				wArgs.FinishedReduceIdx = -1
				call("Coordinator.RequestAndReport", &wArgs, &cReply)
				//Rewind value
				wArgs = WorkerArgs{
					FinishedMapIdx:    -1,
					FinishedReduceIdx: -1,
				}
				cReply = CoordinatorReply{}
			} else {
				//Do reduce
				nMap := cReply.NMap
				reduceIdx := cReply.ReduceIdx
				//get all intermediate kv pairs
				intermediate := []KeyValue{}
				for i := 0; i < nMap; i++ {
					fileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceIdx)
					file, err := os.Open(fileName)
					if err != nil {
						log.Fatalf("cannot read 1 %v", fileName)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}
					err = file.Close()
					if err != nil {
						log.Fatalf("cannot close %v", fileName)
					}
				}
				//shuffle
				sort.Sort(ByKey(intermediate))
				//create output file
				fileName := "mr-out-" + strconv.Itoa(reduceIdx)
				file, err := ioutil.TempFile("", fileName)
				if err != nil {
					log.Fatalf("cannot open output %v", fileName)
				}
				//reduce & write
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

					i = j
				}
				tmpFileName := file.Name()
				err = os.Rename(file.Name(), fileName)
				if err != nil {
					log.Fatalf("cannot rename %v to %v", tmpFileName, fileName)
				}
				file.Close()
				for i := 0; i < cReply.NMap; i++ {
					iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(cReply.ReduceIdx)
					err := os.Remove(iname)
					if err != nil {
						log.Fatalf("cannot open delete" + iname)
					}
				}
				//
				//Report to coordinator
				wArgs.FinishedMapIdx = -1
				wArgs.FinishedReduceIdx = reduceIdx
				call("Coordinator.RequestAndReport", &wArgs, &cReply)
				wArgs = WorkerArgs{
					FinishedMapIdx:    -1,
					FinishedReduceIdx: -1,
				}
				cReply = CoordinatorReply{}
			}
		}
		time.Sleep(time.Second)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
