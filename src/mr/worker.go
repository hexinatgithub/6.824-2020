package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	jobInfo := GetJobInfo()

	// map phase
	mapTask := GetMapTask()
	for mapTask != nil && !mapTask.Exit {
		files := make([]*os.File, 0, jobInfo.NReduce)
		for i := 0; i < jobInfo.NReduce; i++ {
			file, err := ioutil.TempFile(".",
				fmt.Sprintf("mr-%d-%d-*", mapTask.TaskNum, i))
			if err != nil {
				log.Fatal(err)
			}
			files = append(files, file)
		}

		file, err := os.Open(mapTask.FileName)
		if err != nil {
			log.Fatalf("cannot open %v", mapTask.FileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", mapTask.FileName)
		}
		if err = file.Close(); err != nil {
			log.Println(err)
		}
		kva := mapf(mapTask.FileName, string(content))
		for _, kv := range kva {
			file = files[ihash(kv.Key)%jobInfo.NReduce]
			encoder := json.NewEncoder(file)
			err := encoder.Encode(&kv)
			if err != nil {
				log.Fatalf("Write %s file failed!\n", file.Name())
			}
		}

		imf := make([]string, 0, jobInfo.NReduce)
		for i, file := range files {
			name := file.Name()
			if err := file.Close(); err != nil {
				log.Println(err)
			}
			newName := fmt.Sprintf("mr-%d-%d", mapTask.TaskNum, i)
			if err = os.Rename(name, newName); err != nil {
				log.Println(err)
			}
			imf = append(imf, newName)
		}

		info := &IntermediateInfo{
			MapTask:          *mapTask,
			IntermediateFile: imf,
		}
		MapTaskDone(info)
		mapTask = GetMapTask()
	}

	//reduce phase
	reduceTask := GetReduceTask()
	for reduceTask != nil && !reduceTask.Exist {
		var intermediate []KeyValue
		for _, fn := range reduceTask.IntermediateFiles {
			file, err := os.Open(fn)
			if err != nil {
				log.Fatalf("Open %s file failed!", file.Name())
			}
			decoder := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := decoder.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			if err := file.Close(); err != nil {
				log.Println(err)
			}
		}

		sort.Sort(ByKey(intermediate))

		tempName := fmt.Sprintf("mr-out-%d-*", reduceTask.TaskNum)
		tempFile, err := ioutil.TempFile(".", tempName)
		if err != nil {
			log.Fatal(err)
		}

		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			var values []string
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		tempPath := tempFile.Name()
		newPath := fmt.Sprintf("mr-out-%d", reduceTask.TaskNum)
		if err = tempFile.Close(); err != nil {
			log.Println(err)
		}
		if err = os.Rename(tempPath, newPath); err != nil {
			log.Fatal(err)
		}

		ReduceTaskDone(&ReduceTaskDoneInfo{
			TaskNum:    reduceTask.TaskNum,
			OutputFile: newPath,
		})
		reduceTask = GetReduceTask()
	}
}

func GetReduceTask() *ReduceTask {
	reply := new(ReduceTask)
	if !call("Master.GetReduceTask", &struct{}{}, &reply) {
		os.Exit(1)
	}
	return reply
}

func ReduceTaskDone(info *ReduceTaskDoneInfo) {
	if !call("Master.ReduceTaskDone", info, &struct{}{}) {
		os.Exit(1)
	}
}

func MapTaskDone(info *IntermediateInfo) {
	if !call("Master.MapTaskDone", info, &struct{}{}) {
		os.Exit(1)
	}
	return
}

func GetJobInfo() *JobInfo {
	reply := new(JobInfo)
	if !call("Master.JobInfo", &struct{}{}, &reply) {
		os.Exit(1)
	}
	return reply
}

func GetMapTask() *MapTask {
	reply := new(MapTask)
	if !call("Master.GetMapTask", &struct{}{}, reply) {
		os.Exit(1)
	}
	return reply
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
