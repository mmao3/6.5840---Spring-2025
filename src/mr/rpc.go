package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "time"

type TaskStatus int

const (
	Available TaskStatus = iota
	Completed
	Pending
)

// Define the type of task
type TaskType int

const (
	Exit TaskType = iota
	Map
	Reduce
	Waiting
)

// Struct representing a single task
type MapReduceTask struct {
	TaskType    TaskType
	TaskStatus  TaskStatus
	TaskId      int
	StartTime   time.Time
	EndTime     time.Time
	InputFiles  []string
	OutputFiles []string
}

// Struct used by worker to receive a task
type RequestTaskReply struct {
	Task     MapReduceTask
	NReduce  int
}

type Empty struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
