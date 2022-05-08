package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//
import (
	"os"
	"strconv"
)

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

type TaskAssignArgs struct {
	Name string
}

type TaskAssignReply struct {
	HasTask         bool
	JobStatus       JobStatus
	MapTaskReply    MapTaskReply
	ReduceTaskReply ReduceTaskReply
}

type MapTaskReply struct {
	TaskNum int
	Input   string
	Output  []string
	Worker  string
	NReduce int
}

type ReduceTaskReply struct {
	TaskNum int
	Inputs  []string
	Output  string
	Worker  string
}

type TaskFinishArgs TaskAssignReply

type TaskFinishReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket Name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
