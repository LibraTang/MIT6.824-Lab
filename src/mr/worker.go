package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
	// 启动worker
	for {
		// worker从master获取任务
		task := getTask()
		// 根据task的state,将map任务交给mapper,reduce任务交给reducer
		// golang的switch默认情况下每个case自带break
		switch task.TaskState {
		case Map:
			mapper(&task, mapf)
		case Reduce:
			reducer(&task, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}
	}
}

//
// worker向master发送rpc请求task
//
func getTask() Task {
	args := ExampleArgs{}
	reply := Task{}

	// 发送rpc请求
	call("Master.AssignTask", &args, &reply)

	return reply
}

//
// 处理map任务
//
func mapper(task *Task, mapf func(string, string) []KeyValue) {
	// 读取文件内容
	content, err := os.ReadFile(task.Input)
	if err != nil {
		log.Fatal("Failed to read file: "+task.Input, err)
	}
	// content交给mapf处理,并缓存中间结果
	intermediates := mapf(task.Input, string(content))

	// 缓存的中间结果保存在本地磁盘,并切分成R份(reducer的数量)
	// 根据key作为hash切分
	buffer := make([][]KeyValue, task.NReducer)
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.NReducer
		buffer[slot] = append(buffer[slot], intermediate)
	}
	mapOutput := make([]string, 0)
	for i := 0; i < task.NReducer; i++ {
		mapOutput = append(mapOutput, writeToLocalFile(task.TaskNumber, i, &buffer[i]))
	}
	// 将切分后的R份文件位置发送给master
	task.Intermediates = mapOutput
	taskCompleted(task)
}

func reducer(task *Task, reducef func(string, []string) string) {

}

//
// 将map任务产生的临时中间文件写入磁盘，并返回文件位置
// 命名格式为"mr-mapTaskNumber-reduceTaskNumber"
//
func writeToLocalFile(mapTaskNumber int, reduceTaskNumber int, intermediate *[]KeyValue) string {
	// 获取当前路径
	dir, _ := os.Getwd()
	// 创建临时文件，防止一台机器crash的时候被其他机器观察到写了一半的文件
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	// 以Json文件的形式保存
	enc := json.NewEncoder(tempFile)
	for _, kv := range *intermediate {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempFile.Close()
	// 重命名文件
	outputName := fmt.Sprintf("mr-%d-%d", mapTaskNumber, reduceTaskNumber)
	os.Rename(tempFile.Name(), outputName)
	return filepath.Join(dir, outputName)
}

//
// worker任务完成后通知master
//
func taskCompleted(task *Task) {
	reply := ExampleReply{}
	call("Master.TaskCompleted", task, &reply)
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
