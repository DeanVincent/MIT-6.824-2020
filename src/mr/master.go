package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type JobStatus int
type TaskStatus int

const (
	JOB_MAP       = iota
	JOB_REDUCE    = iota
	JOB_COMPLETED = iota
)

const (
	TASK_NOT_STARTED = iota
	TASK_IN_PROGRESS = iota
	TASK_COMPLETED   = iota
)

type Master struct {
	// todo: Your definitions here.
	mux sync.Mutex // 锁，只允许一个线程在修改Coordinator的状态

	JobStatus JobStatus

	mapTasks    []MapTask
	reduceTasks []ReduceTask

	//splits  []string   // 输入分片, 分片下标与mapTask下标一一对应
	//inters  [][]string // 中间结果, 外层数组下标为reduce任务编号, 内层数组存放redece任务待读取的结果, 添加元素时使用append(m.inters[], xxx)
	//outputs []string   // 最终结果, 下标为reduce任务编号, 添加元素时使用下标直接添加
}

// 对于每一个split分配一个map task
// 如果一个map task运行了10s依然没有结束的话, 就视为死亡, 为此 split 另外启动一个map task, 谁先结束算谁的
// 如果所有map task的状态都是已完成, 那么可以开始reduce task了

/*
对于map task的操作
1. 根据split查询map task(状态 worker进程), 每个split可能有不止一个worker
2. 遍历未完成的map task, 如果超时则重启一个; 如果所有均已完成,可以开始reduce task
3. 每个map task完成时, 会发送 R 个中间文件, 并且将此任务标记为 done

存储:
1. 每个split对应的map任务的状态(未开始\进行中\已完成)
2. 每个已完成map任务输出的中间文件(数量为: M x R)
*/

type MapTask struct {
	input   string
	outputs []string
	status  TaskStatus
	workers []Workerm
}

type Workerm struct {
	name  string
	start time.Time
}

type ReduceTask struct {
	inputs  []string
	output  string
	status  TaskStatus
	workers []Workerm
}

// Your code here -- RPC handlers for the Worker to call.
func (m *Master) AssignTask(args *TaskAssignArgs, reply *TaskAssignReply) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	reply.JobStatus = m.JobStatus

	if m.JobStatus == JOB_MAP { // job在map阶段
		tasks := m.mapTasks

		for i := range tasks { // 检查每个map task需不需要新的worker执行
			if tasks[i].assignCondition() {
				tasks[i].status = TASK_IN_PROGRESS
				tasks[i].workers = append(tasks[i].workers, Workerm{name: args.Name, start: time.Now()})

				reply.HasTask = true
				reply.MapTaskReply.TaskNum = i
				reply.MapTaskReply.Worker = args.Name
				reply.MapTaskReply.Input = tasks[i].input
				reply.MapTaskReply.NReduce = len(m.reduceTasks)
				log.Printf("master assign map task %d to worker %s, input %s\n",
					i, args.Name, tasks[i].input)
				break
			}
		}

	} else if m.JobStatus == JOB_REDUCE {
		tasks := m.reduceTasks

		for i := range tasks {
			if tasks[i].assignCondition() {
				tasks[i].status = TASK_IN_PROGRESS
				tasks[i].workers = append(tasks[i].workers, Workerm{name: args.Name, start: time.Now()})

				reply.HasTask = true
				reply.ReduceTaskReply.TaskNum = i
				reply.ReduceTaskReply.Worker = args.Name
				reply.ReduceTaskReply.Inputs = make([]string, len(tasks[i].inputs))
				copy(reply.ReduceTaskReply.Inputs, tasks[i].inputs)
				log.Printf("master assign reduce task %d to worker %s, input %v\n",
					i, args.Name, tasks[i].inputs)
				break
			}
		}
	}

	return nil
}

func (t *MapTask) assignCondition() bool {
	if t.status == TASK_NOT_STARTED {
		return true
	} else if t.status == TASK_IN_PROGRESS {
		allTimeOut := true
		for _, worker := range t.workers {
			if time.Now().Sub(worker.start) < 10*time.Second { // 存在一个合理的执行中的worker
				allTimeOut = false
				break
			}
		}
		return allTimeOut
	}
	return false
}

func (t *ReduceTask) assignCondition() bool {
	if t.status == TASK_NOT_STARTED {
		return true
	} else if t.status == TASK_IN_PROGRESS {
		allTimeOut := true
		for _, worker := range t.workers {
			if time.Now().Sub(worker.start) < 10*time.Second { // 存在一个合理的执行中的worker
				allTimeOut = false
				break
			}
		}
		return allTimeOut
	}
	return false
}

func (m *Master) FinishTask(args *TaskFinishArgs, reply *TaskFinishReply) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	if m.JobStatus != args.JobStatus {
		return nil
	}

	if args.JobStatus == JOB_MAP {
		taskReply := args.MapTaskReply
		task := &m.mapTasks[taskReply.TaskNum]
		task.status = TASK_COMPLETED
		task.outputs = make([]string, len(taskReply.Output))
		copy(task.outputs, taskReply.Output)
		log.Printf("master has received that map task %d has been finished by worker %s, input %v, outputs %v\n",
			taskReply.TaskNum, taskReply.Worker, taskReply.Input, taskReply.Output)

		mapComplete := true
		for _, mapTask := range m.mapTasks {
			if mapTask.status != TASK_COMPLETED {
				mapComplete = false
				break
			}
		}
		if mapComplete { // 进入reduce阶段
			m.JobStatus = JOB_REDUCE
			for _, mapTask := range m.mapTasks {
				for i, outputs := range mapTask.outputs {
					if outputs != "" {
						m.reduceTasks[i].inputs = append(m.reduceTasks[i].inputs, outputs)
					}
				}
			}
			log.Printf("--------------------  map status complete --------------------  \n")
			for i, reduceTask := range m.reduceTasks {
				log.Printf("reduce task %v inputs %v \n", i, reduceTask.inputs)
			}
		}
	} else if args.JobStatus == JOB_REDUCE {
		taskReply := args.ReduceTaskReply
		task := &m.reduceTasks[taskReply.TaskNum]
		task.status = TASK_COMPLETED
		task.output = taskReply.Output
		log.Printf("master has received that reduce task %d has been finished by worker %s, inputs %v, output %v\n",
			taskReply.TaskNum, taskReply.Worker, taskReply.Inputs, taskReply.Output)

		reduceComplete := true
		for _, reduceTask := range m.reduceTasks {
			if reduceTask.status != TASK_COMPLETED {
				reduceComplete = false
				break
			}
		}
		if reduceComplete {
			m.JobStatus = JOB_COMPLETED
			log.Printf("master job reduce status complete\n")
		}
	}
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
// start a thread that listens for RPCs from Worker.go
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
	if m.JobStatus == JOB_COMPLETED {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	// 初始化master
	m.JobStatus = JOB_MAP

	m.mapTasks = make([]MapTask, len(files))
	for i := 0; i < len(files); i++ {
		m.mapTasks[i] = MapTask{input: files[i],
			status:  TASK_NOT_STARTED,
			outputs: make([]string, nReduce),
			workers: []Workerm{}}
	}

	m.reduceTasks = make([]ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = ReduceTask{inputs: []string{},
			status:  TASK_NOT_STARTED,
			workers: []Workerm{}}
	}

	m.server()
	return &m
}
