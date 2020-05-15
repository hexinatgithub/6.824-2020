package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	ReduceNum       int
	TotalMapTask    int
	TotalReduceTask int

	mapTaskQueue            []*MapTask
	mapTaskStatus           map[int]Status
	mapTaskTimer            map[int]*time.Timer
	MapTaskIntermediateFile map[int][]string

	reduceTaskQueue      []*ReduceTask
	reduceTaskStatus     map[int]Status
	reduceTaskTimer      map[int]*time.Timer
	ReduceTaskOutputFile map[int]string

	Phase Phase

	cond *sync.Cond
}

// Your code here -- RPC handlers for the worker to call.

type Status string
type Phase string

const (
	Idle       = Status("Idle")
	InProgress = Status("InProgress")
	Completed  = Status("Completed")

	MapPhase      = Phase("MapPhase")
	ReducePhase   = Phase("ReducePhase")
	FinishedPhase = Phase("FinishedPhase")

	MapTaskTimeOut    = time.Second * 10
	ReduceTaskTimeOut = time.Second * 10
)

// GetMapTask return not finished map task,
// or nil if all map task are finished.
func (m *Master) GetMapTask(_ *struct{}, reply *MapTask) error {
	m.cond.L.Lock()
	for {
		if m.Phase != MapPhase {
			m.cond.L.Unlock()
			reply.Exit = true
			return nil
		}

		if len(m.mapTaskQueue) == 0 {
			m.cond.Wait()
			continue
		}

		task := m.mapTaskQueue[0]
		*reply = *task
		newMapTaskQueue := make([]*MapTask, 0, len(m.mapTaskQueue)-1)
		copy(newMapTaskQueue, m.mapTaskQueue[1:])
		m.mapTaskQueue = newMapTaskQueue
		m.mapTaskStatus[task.TaskNum] = InProgress
		m.mapTaskTimer[task.TaskNum] = time.AfterFunc(MapTaskTimeOut, func() {
			m.cond.L.Lock()
			defer m.cond.L.Unlock()
			if m.Phase == MapPhase && m.mapTaskStatus[task.TaskNum] ==
				InProgress {
				delete(m.mapTaskTimer, task.TaskNum)
				m.mapTaskStatus[task.TaskNum] = Idle
				m.mapTaskQueue = append(m.mapTaskQueue, task)
				m.cond.Signal()
			}
		})
		m.cond.L.Unlock()
		return nil
	}
}

// MapTaskDone return true if job not be done by other worker
func (m *Master) MapTaskDone(args *IntermediateInfo, _ *struct{}) error {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()
	if m.Phase != MapPhase {
		return nil
	}

	taskNum := args.TaskNum
	timer, ok := m.mapTaskTimer[taskNum]
	if ok {
		timer.Stop()
		delete(m.mapTaskTimer, taskNum)
	}

	_, ok = m.MapTaskIntermediateFile[taskNum]
	if ok {
		return nil
	}
	m.mapTaskStatus[taskNum] = Completed
	m.MapTaskIntermediateFile[taskNum] = args.IntermediateFile

	if len(m.MapTaskIntermediateFile) == m.TotalMapTask {
		m.Phase = ReducePhase
		m.mapTaskStatus = nil
		m.mapTaskQueue = nil
		m.mapTaskTimer = nil
		m.cond.Broadcast()

		// initial reduce phase task state
		for i := 0; i < m.ReduceNum; i++ {
			var reduceTaskFiles []string
			for _, ifs := range m.MapTaskIntermediateFile {
				reduceTaskFiles = append(reduceTaskFiles, ifs[i])
			}
			m.reduceTaskQueue = append(m.reduceTaskQueue, &ReduceTask{
				TaskNum:           i,
				IntermediateFiles: reduceTaskFiles,
			})
			m.reduceTaskStatus[i] = Idle
		}
	}
	return nil
}

func (m *Master) GetReduceTask(_ *struct{}, reply *ReduceTask) error {
	m.cond.L.Lock()
	for {
		if m.Phase != ReducePhase {
			m.cond.L.Unlock()
			reply.Exist = true
			return nil
		}

		if len(m.reduceTaskQueue) == 0 {
			m.cond.Wait()
			continue
		}

		task := m.reduceTaskQueue[0]
		*reply = *task
		newReduceQueue := make([]*ReduceTask, 0, len(m.reduceTaskQueue)-1)
		copy(newReduceQueue, m.reduceTaskQueue[1:])
		m.reduceTaskQueue = newReduceQueue
		m.reduceTaskStatus[task.TaskNum] = InProgress
		m.reduceTaskTimer[task.TaskNum] = time.AfterFunc(ReduceTaskTimeOut, func() {
			m.cond.L.Lock()
			defer m.cond.L.Unlock()
			if m.Phase == ReducePhase && m.reduceTaskStatus[task.TaskNum] ==
				InProgress {
				delete(m.reduceTaskTimer, task.TaskNum)
				m.reduceTaskStatus[task.TaskNum] = Idle
				m.reduceTaskQueue = append(m.reduceTaskQueue, task)
				m.cond.Signal()
			}
		})
		m.cond.L.Unlock()
		return nil
	}
}

func (m *Master) ReduceTaskDone(args *ReduceTaskDoneInfo, _ *struct{}) error {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	if m.Phase != ReducePhase {
		return nil
	}

	taskNum := args.TaskNum
	timer, ok := m.reduceTaskTimer[taskNum]
	if ok {
		timer.Stop()
		delete(m.reduceTaskTimer, taskNum)
	}

	_, ok = m.ReduceTaskOutputFile[taskNum]
	if ok {
		return nil
	}
	m.ReduceTaskOutputFile[taskNum] = args.OutputFile
	m.reduceTaskStatus[taskNum] = Completed

	if len(m.ReduceTaskOutputFile) == m.ReduceNum {
		m.Phase = FinishedPhase
		m.reduceTaskQueue = nil
		m.reduceTaskStatus = nil
		m.reduceTaskTimer = nil
		m.cond.Broadcast()
	}
	return nil
}

func (m *Master) JobInfo(_ *struct{}, reply *JobInfo) error {
	reply.NReduce = m.ReduceNum
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.cond.L.Lock()
	defer m.cond.L.Unlock()
	ret = m.Phase == FinishedPhase
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.ReduceNum = nReduce
	m.TotalMapTask = len(files)

	m.mapTaskStatus = make(map[int]Status)
	for i, fn := range files {
		m.mapTaskQueue = append(m.mapTaskQueue, &MapTask{
			TaskNum:  i,
			FileName: fn,
		})
		m.mapTaskStatus[i] = Idle
	}
	m.mapTaskTimer = make(map[int]*time.Timer)
	m.MapTaskIntermediateFile = make(map[int][]string)

	m.reduceTaskQueue = make([]*ReduceTask, 0, nReduce)
	m.reduceTaskStatus = make(map[int]Status)
	m.ReduceTaskOutputFile = make(map[int]string)
	m.reduceTaskTimer = make(map[int]*time.Timer)

	m.Phase = MapPhase
	m.cond = sync.NewCond(&sync.Mutex{})

	m.server()
	return &m
}
