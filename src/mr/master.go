package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Master struct {
	// Your definitions here.
	mapTaskWaiting TaskStatQueue
	mapTaskRunning TaskStatQueue

	reduceTaskWaiting TaskStatQueue
	reduceTaskRunning TaskStatQueue

	isDone bool
}

// TaskStatInterface

type TaskStatInterface interface {
	GenerateTaskInfo() TaskInfo
	OutOfTime() bool
	GetFileIndex() int
	GetPartIndex() int
	SetNow()
}

type TaskStat struct {
	beginTime time.Time
	filename  string
	fileIndex int
	partIndex int
	nReduce   int
	nFiles    int
}

type MapTaskStat struct {
	TaskStat
}

type ReduceTaskStat struct {
	TaskStat
}

// TaskStatQueue TaskQueue
type TaskStatQueue struct {
	taskArray []TaskStatInterface
	mutex     sync.Mutex
}

func (queue *TaskStatQueue) lock() {
	queue.mutex.Lock()
}

func (queue *TaskStatQueue) unlock() {
	queue.mutex.Unlock()
}

func (queue *TaskStatQueue) size() int {
	return len(queue.taskArray)
}

func (queue *TaskStatQueue) pop() TaskStatInterface {
	queue.lock()
	if queue.size() == 0 {
		queue.unlock()
		return nil
	}

	res := queue.taskArray[queue.size()-1]
	queue.taskArray = queue.taskArray[:queue.size()-1]
	queue.unlock()
	return res
}

func (queue *TaskStatQueue) push(task TaskStatInterface) {
	queue.lock()
	if task == nil {
		queue.unlock()
		return
	}
	queue.taskArray = append(queue.taskArray, task)
	queue.unlock()
}

func (queue *TaskStatQueue) RemoveTask(fileIndex int, partIndex int) {

}

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

func (m *Master) TaskDone(args *TaskInfo, reply *ExampleReply) error {
	switch args.State {
	case TaskMap:
		fmt.Printf("Map task on %vth file %v complete\n", args.FileIndex, args.FileName)
		m.mapTaskRunning.RemoveTask(args.FileIndex, args.PartIndex)
		// 所有 map 任务完成，开始分配 reduce 任务
		if m.mapTaskWaiting.size() == 0 && m.mapTaskRunning.size() == 0 {
			m.distributeReduceTask()
		}
		break
	case TaskReduce:
		fmt.Printf("Reduce task on %vth part complete\n", args.PartIndex)
		m.reduceTaskRunning.RemoveTask(args.FileIndex, args.PartIndex)
		break
	default:
		panic("Unknown state")
	}
	return nil
}

func (m *Master) AskTask(args *ExampleArgs, reply *TaskInfo) error {
	// 是否有可用的reduce任务
	reduceTask := m.reduceTaskWaiting.pop()
	if reduceTask != nil {
		// 记录任务开始时间
		reduceTask.SetNow()
		m.mapTaskRunning.push(reduceTask)
		*reply = reduceTask.GenerateTaskInfo()
		fmt.Printf("Distributing reduce task on part %v %vth file %v\n", reply.PartIndex, reply.FileIndex, reply.FileName)
		return nil
	}

	// 是否有可用的map任务
	mapTask := m.mapTaskWaiting.pop()
	if mapTask != nil {
		// 记录任务开始时间
		mapTask.SetNow()
		m.mapTaskRunning.push(mapTask)
		*reply = mapTask.GenerateTaskInfo()
		fmt.Printf("Distributing map task on %vth file %v\n", reply.FileIndex, reply.FileName)
		return nil
	}

	// should exit
	// 所有任务已分配
	if m.mapTaskRunning.size() > 0 || m.reduceTaskRunning.size() > 0 {
		reply.State = TaskWait
		return nil
	}

	// 所有任务已完成
	reply.State = TaskEnd
	m.isDone = true
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//

func (m *Master) server() {
	err := rpc.Register(m)
	if err != nil {
		return
	}
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	SocketName := masterSock()
	err = os.Remove(SocketName)
	if err != nil {
		return
	}
	l, e := net.Listen("unix", SocketName)
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

// 分发 Reduce 任务
func (m *Master) distributeReduceTask() {

}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{isDone: false}
	// Your code here.

	m.server()
	return &m
}
