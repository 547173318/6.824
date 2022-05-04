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

var timeout = 10 * time.Second

type Master struct {
	mu   sync.Mutex
	cond sync.Cond

	mapFinished    []bool
	reduceFinished []bool
	mapIssued      []time.Time
	reduceIssued   []time.Time

	isDone bool
}

// HandleGetTask 由 worker 调用
func (m *Master) HandleGetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	fmt.Println("HandleGetTask start...")
	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. check Map task first
	for {
		mapDone := true
		for idx, done := range m.mapFinished {
			// this task is issued or do nothing yet
			if !done {
				fmt.Println("hello1")
				mapDone = false // There are still some tasks waiting to be completed
				beginTime := m.mapIssued[idx]
				// issue task if did not issue or timeout
				if beginTime.IsZero() || time.Now().Sub(beginTime) > timeout {
					fmt.Println("hello2")
					reply.TaskType = Map
					reply.TaskNum = idx
					m.mu.Unlock()
					fmt.Println("HandleGetTask end --- type=%v, num=%v", reply.TaskType, reply.TaskNum)
					return nil
				}
			}
		}
		// There are still some tasks waiting to be completed.
		// wait
		if !mapDone {
			m.cond.Wait()
		} else {
			break
		}
	}
	// 2. then check Reduce task
	for {
		reduceDone := true
		for idx, done := range m.reduceFinished {
			// this task is issued or do nothing yet
			if !done {
				reduceDone = false // There are still some tasks waiting to be completed
				beginTime := m.reduceIssued[idx]
				// issue task if did not issue or timeout
				if beginTime.IsZero() || time.Now().Sub(beginTime) > timeout {
					reply.TaskType = Reduce
					reply.TaskNum = idx
					m.mu.Unlock()
					fmt.Println("HandleGetTask end --- type=%v, num=%v", reply.TaskType, reply.TaskNum)
					return nil
				}
			}
		}
		// There are still some tasks waiting to be completed.
		// wait
		if !reduceDone {
			m.cond.Wait()
		} else {
			break
		}
	}
	// 3. all task are completed
	reply.TaskType = 3
	m.isDone = true
	fmt.Println("HandleGetTask end --- type=%v, num=%v", reply.TaskType, reply.TaskNum)
	return nil
}

// HandleFinished 由 worker 调用
func (m *Master) HandleFinished(args *FinishedArgs, reply *FinishedReply) error {
	fmt.Println("HandleFinished start...")
	m.mu.Lock()
	defer m.mu.Unlock()
	switch args.TaskType {
	case Map:
		m.mapFinished[args.TaskNum] = true
	case Reduce:
		m.reduceFinished[args.TaskNum] = true
	}
	m.cond.Broadcast()
	fmt.Println("HandleFinished end --- type=%v, num=%v", args.TaskType, args.TaskNum)
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done :
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// MakeMaster :
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// todo
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//intermediate := []KeyValue{}
	//for _, filename := range os.Args[2:] {
	//	file, err := os.Open(filename)
	//	if err != nil {
	//		log.Fatalf("cannot open %v", filename)
	//	}
	//	content, err := ioutil.ReadAll(file)
	//	if err != nil {
	//		log.Fatalf("cannot read %v", filename)
	//	}
	//	file.Close()
	//	kva := mapf(filename, string(content))
	//	intermediate = append(intermediate, kva...)
	//}

	m.server()
	return &m
}
