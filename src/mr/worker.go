package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
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

func getInputContent(name string) string {
	file, err := os.Open(name)
	if err != nil {
		log.Fatalf("cannot open %v", name)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", name)
	}
	file.Close()
	return string(content)
}

var pid int

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your Worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	pid = os.Getpid()
	for {
		task := CallAssignTask()
		if !task.HasTask {
			// do nothing
		} else if task.JobStatus == JOB_MAP {
			taskId := task.MapTaskReply.TaskNum
			filename := task.MapTaskReply.Input
			log.Printf("worker %d start map task %d, input %s\n", pid, taskId, filename)

			content := getInputContent(filename)

			kva := mapf(filename, content)

			r := task.MapTaskReply.NReduce
			intermediates := make([][]KeyValue, r)
			for _, kv := range kva {
				idx := ihash(kv.Key) % r
				intermediates[idx] = append(intermediates[idx], kv)
			}
			task.MapTaskReply.Output = make([]string, r)
			pwd, _ := os.Getwd()
			for i, intermediate := range intermediates {
				sort.Sort(ByKey(intermediate))
				f, _ := ioutil.TempFile(pwd, "*")
				//log.Printf("create temp file %v\n", f.Name())
				b, _ := json.Marshal(intermediate)
				f.Write(b)
				f.Sync()
				f.Close()
				oname := "mr-" + strconv.Itoa(task.MapTaskReply.TaskNum) + "-" + strconv.Itoa(i)
				os.Rename(f.Name(), oname)
				task.MapTaskReply.Output[i] = oname
			}
			taskFinishArgs := TaskFinishArgs(task)
			CallFinishTask(&taskFinishArgs)
			log.Printf("worker %d finish map task %d, outputs %v\n", pid, taskId, task.MapTaskReply.Output)
		} else if task.JobStatus == JOB_REDUCE {
			taskId := task.ReduceTaskReply.TaskNum
			inputs := task.ReduceTaskReply.Inputs
			log.Printf("worker %d start reduce task %d, inputs %v\n", pid, taskId, inputs)

			kva := make([][]KeyValue, len(inputs))

			// json to kv's
			for i, ifname := range inputs {
				file, err := os.Open(ifname)
				if err != nil {
					log.Fatalf("cannot open %v", ifname)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", ifname)
				}
				file.Close()
				json.Unmarshal(content, &kva[i])
			}

			// kv's to map[k]{v1 v2 v3}
			kvmap := make(map[string][]string)
			for _, kvs := range kva {
				for _, kv := range kvs {
					kvmap[kv.Key] = append(kvmap[kv.Key], kv.Value)
				}
			}

			pwd, _ := os.Getwd()
			f, _ := ioutil.TempFile(pwd, "*")
			//log.Printf("create temp file %v\n", f.Name())
			for k, v := range kvmap {
				fmt.Fprintf(f, "%v %v\n", k, reducef(k, v))
			}
			f.Close()
			oname := "mr-out-" + strconv.Itoa(task.ReduceTaskReply.TaskNum)
			//log.Printf("rename temp file %v to %v\n", f.Name(), oname)
			os.Rename(f.Name(), oname)
			task.ReduceTaskReply.Output = oname
			taskFinishArgs := TaskFinishArgs(task)
			CallFinishTask(&taskFinishArgs)
			//for _, input := range inputs {
			//	os.Remove(input)
			//}
			log.Printf("worker %d finish reduce task %d, output %s\n", pid, taskId, task.ReduceTaskReply.Output)
		} else if task.JobStatus == JOB_COMPLETED {
			break
		}
		time.Sleep(time.Second * 1)
	}
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

func CallAssignTask() TaskAssignReply {
	args := TaskAssignArgs{Name: "worker" + strconv.Itoa(pid)}
	reply := TaskAssignReply{}
	call("Master.AssignTask", &args, &reply)
	if reply.HasTask {
		log.Printf("CallAssignTask rpc reply %v\n", reply)
	}
	return reply
}

func CallFinishTask(args *TaskFinishArgs) {
	reply := TaskFinishReply{}
	call("Master.FinishTask", &args, &reply)
	log.Printf("CallFinishTask rpc args %v, reply %v\n", args, reply)
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

	fmt.Printf("call error: %s\n", err)
	return false
}
