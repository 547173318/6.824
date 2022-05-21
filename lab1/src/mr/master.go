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

var timeout = 60 * time.Second

type Master struct {
	mu   sync.Mutex
	cond *sync.Cond

	inputFiles []string
	nFile      int
	nReduce    int

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

	defer func() {
		fmt.Printf("HandleGetTask end --- type=%v, num=%v, filename=%v\n", reply.TaskType, reply.TaskNum, reply.FileName)
	}()

	// 1. check Map task first
	for {
		mapDone := true
		for idx, done := range m.mapFinished {
			// this task is issued or do nothing yet
			if !done {
				beginTime := m.mapIssued[idx]
				// issue task if did not issue or timeout
				if beginTime.IsZero() || time.Now().Sub(beginTime) > timeout {
					reply.TaskType = Map
					reply.TaskNum = idx
					reply.FileName = m.inputFiles[idx]
					reply.NReduce = m.nReduce
					m.mapIssued[idx] = time.Now()
					return nil
				} else {
					mapDone = false // There are still some tasks waiting to be completed
				}
			}
		}
		// There are still some tasks waiting to be completed.
		// wait
		if !mapDone {
			fmt.Println("waiting...")
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
				beginTime := m.reduceIssued[idx]
				// issue task if did not issue or timeout
				if beginTime.IsZero() || time.Now().Sub(beginTime) > timeout {
					reply.TaskType = Reduce
					reply.TaskNum = idx
					m.reduceIssued[idx] = time.Now()
					return nil
				} else {
					reduceDone = false // There are still some tasks waiting to be completed
				}
			}
		}
		// There are still some tasks waiting to be completed.
		// wait
		if !reduceDone {
			fmt.Println("waiting...")
			m.cond.Wait()
		} else {
			break
		}
	}
	// 3. all task are completed
	reply.TaskType = 3
	m.isDone = true
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
	fmt.Printf("HandleFinished end --- type=%v, num=%v\n", args.TaskType, args.TaskNum)
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
	m.nFile = len(files)
	m.nReduce = nReduce
	//m.nFile = 2
	//m.nReduce = 2

	// init
	m.inputFiles = files
	m.mapFinished = make([]bool, m.nFile)
	m.mapIssued = make([]time.Time, m.nFile)
	m.reduceFinished = make([]bool, m.nReduce)
	m.reduceIssued = make([]time.Time, m.nReduce)
	m.cond = sync.NewCond(&m.mu)

	m.server()
	fmt.Printf("nFile = %v, nReduce = %v\n", m.nFile, m.nReduce)
	return &m
}
