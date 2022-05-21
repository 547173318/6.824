package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskType int

const (
	Map    TaskType = 1
	Reduce TaskType = 2
	Done   TaskType = 3
)

type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskType TaskType
	TaskNum  int
	FileName string
	NReduce  int
}

type FinishedArgs struct {
	TaskType TaskType
	TaskNum  int
}

type FinishedReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
