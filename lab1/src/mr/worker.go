package mr

import (
	"fmt"
	"os"
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

// Worker :
// main/mrworker.go calls this function
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// 1.ask for a task from Master
	getTaskArgs := GetTaskArgs{}
	getTaskReply := GetTaskReply{}
	call("Master.HandleGetTask", &getTaskArgs, &getTaskReply)

	// 2.decide to do something by taskType
	switch getTaskReply.TaskType {
	case Map:
		performMap()
	case Reduce:
		performReduce()
	case Done:
		fmt.Println("---os.Exit(1)")
		os.Exit(1)
	}
	// 3.notify Master the task has been finished
	finishedArgs := FinishedArgs{
		TaskType: getTaskReply.TaskType,
		TaskNum:  getTaskReply.TaskNum,
	}
	finishedReply := FinishedReply{}
	call("Master.HandleFinished", &finishedArgs, &finishedReply)

}

func performMap() {
	time.Sleep(1 * time.Second)
	fmt.Println("---performMap end")
}

func performReduce() {
	time.Sleep(1 * time.Second)
	fmt.Println("---performReduce end")
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
