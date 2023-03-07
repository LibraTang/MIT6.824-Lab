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

// Task状态(Map或Reduce)
type MasterTaskStatus int

// Task 和 Master 状态
type State int

// 3种Task状态，自增
const (
	Idle MasterTaskStatus = iota
	InProgress
	Completed
)

// Master 和 Task 共用 State
const (
	Map State = iota
	Reduce
	Exit
	Wait
)

// 任务 Master存储Task的信息
type MasterTask struct {
	TaskStatus    MasterTaskStatus
	StartTime     time.Time
	TaskReference *Task
}

// 任务详情, Map和Reduce共用
type Task struct {
	Input         string
	TaskState     State
	TaskNumber    int
	NReducer      int
	Intermediates []string
	Output        string
}

// Master全局信息
type Master struct {
	// Your definitions here.
	TaskQueue     chan *Task          // 任务队列
	TaskMeta      map[int]*MasterTask // 任务元信息
	MasterPhase   State               // Master所处阶段, task和master应该一致
	NReduce       int
	InputFiles    []string
	Intermediates [][]string // Map任务产生的R个中间文件
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

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
	mu.Lock()
	defer mu.Unlock()
	// 若master转入exit阶段，则所有任务都已经完成
	ret := m.MasterPhase == Exit
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// 初始化Master
	m := Master{
		TaskQueue:     make(chan *Task, max(nReduce, len(files))),
		TaskMeta:      make(map[int]*MasterTask),
		MasterPhase:   Map,
		NReduce:       nReduce,
		InputFiles:    files,
		Intermediates: make([][]string, nReduce),
	}

	// 创建map任务
	m.createMapTask()

	// 启动master服务器
	m.server()

	// 启动一个go routine检查超时任务
	go m.catchTimeout()

	return &m
}

//
// 创建map任务
//
func (m *Master) createMapTask() {
	// 根据传入的file，每个文件对应一个map任务
	for idx, filename := range m.InputFiles {
		task := &Task{
			Input:      filename,
			TaskState:  Map,
			TaskNumber: idx,
			NReducer:   m.NReduce,
		}
		m.TaskQueue <- task
		m.TaskMeta[idx] = &MasterTask{
			TaskStatus:    Idle,
			TaskReference: task,
		}
	}
}

//
// 创建reduce任务
//
func (m *Master) createReduceTask() {
	// 重建任务元信息，取代上一阶段的map任务元信息
	m.TaskMeta = make(map[int]*MasterTask)
	for idx, files := range m.Intermediates {
		task := &Task{
			TaskState:     Reduce,
			TaskNumber:    idx,
			NReducer:      m.NReduce,
			Intermediates: files,
		}
		m.TaskQueue <- task
		m.TaskMeta[idx] = &MasterTask{
			TaskStatus:    Idle,
			TaskReference: task,
		}
	}
}

//
// 检查超时任务
//
func (m *Master) catchTimeout() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if m.MasterPhase == Exit {
			mu.Unlock()
			return
		}
		for _, masterTask := range m.TaskMeta {
			if masterTask.TaskStatus == InProgress && time.Since(masterTask.StartTime) > 10*time.Second {
				// 若存在超时任务，则将该任务放回任务队列，重置任务状态
				m.TaskQueue <- masterTask.TaskReference
				masterTask.TaskStatus = Idle
			}
		}
		mu.Unlock()
	}
}

//
// master监听来自worker的rpc，分配任务
//
func (m *Master) AssignTask(args *ExampleArgs, reply *Task) error {
	// master查看任务队列里是否有task
	mu.Lock()
	defer mu.Unlock()
	if len(m.TaskQueue) > 0 {
		*reply = *<-m.TaskQueue
		// 记录task状态和启动时间
		m.TaskMeta[reply.TaskNumber].TaskStatus = InProgress
		m.TaskMeta[reply.TaskNumber].StartTime = time.Now()
	} else if m.MasterPhase == Exit {
		*reply = Task{TaskState: Exit}
	} else {
		// 若没有task，则让worker等待
		*reply = Task{TaskState: Wait}
	}
	return nil
}

//
// 收到worker发送的完成的task
//
func (m *Master) TaskCompleted(task *Task, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	if task.TaskState != m.MasterPhase || m.TaskMeta[task.TaskNumber].TaskStatus == Completed {
		// 丢弃重复的结果
		return nil
	}
	m.TaskMeta[task.TaskNumber].TaskStatus = Completed
	// 启动一个go routine处理task结果
	go m.processTaskResult(task)
	return nil
}

//
// 每完成一个任务，整理到master信息中，并判断是否要转到下一阶段
//
func (m *Master) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	// 若所有的map任务都完成,则进入reduce阶段
	// 若所有的reduce任务都完成,则exit
	switch task.TaskState {
	case Map:
		// 收集intermediate信息
		for reduceTaskId, filePath := range task.Intermediates {
			m.Intermediates[reduceTaskId] = append(m.Intermediates[reduceTaskId], filePath)
		}
		// 若所有的map任务都完成,则进入reduce阶段
		if m.allTaskDone() {
			m.createReduceTask()
			m.MasterPhase = Reduce
		}
	case Reduce:
		if m.allTaskDone() {
			m.MasterPhase = Exit
		}
	}
}

//
// 检查当前阶段的任务是否都完成
//
func (m *Master) allTaskDone() bool {
	for _, task := range m.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
