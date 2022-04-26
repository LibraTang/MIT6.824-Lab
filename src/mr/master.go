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

// Task状态
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

// 任务
type MasterTask struct {
	TaskStatus    MasterTaskStatus
	StartTime     time.Time
	TaskReference *Task
}

// 任务详情
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
	MasterPhase   State               // Master所处阶段
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
	ret := false

	// Your code here.

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
		taskMeta := Task{
			Input:      filename,
			TaskState:  Map,
			TaskNumber: idx,
			NReducer:   m.NReduce,
		}
		m.TaskQueue <- &taskMeta
		m.TaskMeta[idx] = &MasterTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
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
				m.TaskQueue <- masterTask.TaskReference
				masterTask.TaskStatus = Idle
			}
		}
		mu.Unlock()
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
