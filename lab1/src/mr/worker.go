package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// Worker :
// main/mrworker.go calls this function
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// 1.ask for a task from Master
	getTaskArgs := GetTaskArgs{}
	getTaskReply := GetTaskReply{}
	call("Master.HandleGetTask", &getTaskArgs, &getTaskReply)
	fmt.Printf("worker gets task --- type=%v, num=%v, filename=%v\n", getTaskReply.TaskType, getTaskReply.TaskNum, getTaskReply.FileName)

	// 2.decide to do something by taskType
	switch getTaskReply.TaskType {
	case Map:
		performMap(getTaskReply.FileName, mapf, getTaskReply.NReduce, getTaskReply.TaskNum)
	case Reduce:
		performReduce(reducef, getTaskReply.TaskNum)
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

// read each input file,
// pass it to Map,
// accumulate the intermediate Map output.
func performMap(filename string, mapf func(string, string) []KeyValue, nReduce int, mapTaskNum int) {
	fmt.Println("---performMap start")
	defer func() {
		fmt.Println("---performMap end")
	}()
	// 1.get kv pairs in a specified file
	kva := getkva(filename, mapf)

	// 2.Append key-value pairs to the specified intermediate file according to the hash value
	// 2-1.create temporary files and encoders for each temporary file
	temFiles, encoders := getTmpFiles(nReduce)
	// 2-2.write output kv pairs into temporary files by ihash()
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduce
		_ = encoders[r].Encode(&kv)
	}
	// 2-3.close temporary file
	for _, f := range temFiles {
		f.Close()
	}
	// 2-4.atomically rename temporary files to final intermediate files
	for i, f := range temFiles {
		intermediateFilename := getIntermediateFilename(mapTaskNum, i)
		os.Rename(f.Name(), intermediateFilename)
	}
}

func performReduce(reducef func(string, []string) string, reduceTaskNum int) {
	time.Sleep(30 * time.Second)
	fmt.Println("---performReduce end")

}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// get kv pairs in a specified file
func getkva(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	intermediate := []KeyValue{}
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
	intermediate = append(intermediate, kva...)

	//fmt.Println("---intermediate: ")
	//fmt.Println(intermediate)
	return kva
}

// create temporary files and encoders for each temporary file
func getTmpFiles(nReduce int) (temFiles []*os.File, encoders []*json.Encoder) {
	for i := 0; i < nReduce; i++ {
		path, _ := os.Getwd() // pwd
		temFile, err := ioutil.TempFile(path, "")
		if err != nil {
			log.Fatalf("can not create temporary")
		}
		temFiles = append(temFiles, temFile)
		encoder := json.NewEncoder(temFile)
		encoders = append(encoders, encoder)
	}
	return
}

// A reasonable naming convention for intermediate files is mr-X-Y,
// where X is the Map task number, and Y is the reduce task number
func getIntermediateFilename(mapTaskNum int, reduceTaskNum int) string {
	intermediateFilename := fmt.Sprintf("mr-%d-%d", mapTaskNum, reduceTaskNum)
	return intermediateFilename
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
