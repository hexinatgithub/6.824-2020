package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// Add your RPC definitions here.

type MapTask struct {
	TaskNum  int    `json:"task_num"`
	FileName string `json:"file_name"`
	Exit     bool   `json:"exist"`
}

type IntermediateInfo struct {
	MapTask
	IntermediateFile []string
}

type ReduceTask struct {
	TaskNum           int `json:"task_num"`
	IntermediateFiles []string
	Exist             bool `json:"exist"`
}

type ReduceTaskDoneInfo struct {
	TaskNum    int    `json:"task_num"`
	OutputFile string `json:"output_file"`
}

type JobInfo struct {
	NReduce int `json:"n_reduce"`
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
