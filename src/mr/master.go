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
	fileName  string
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

func (mapTS *MapTaskStat) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		State:     TaskMap,
		FileName:  mapTS.fileName,
		FileIndex: mapTS.fileIndex,
		PartIndex: mapTS.partIndex,
		NReduce:   mapTS.nReduce,
		NFiles:    mapTS.nFiles,
	}
}

func (mapTS *ReduceTaskStat) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		State:     TaskReduce,
		FileName:  mapTS.fileName,
		FileIndex: mapTS.fileIndex,
		PartIndex: mapTS.partIndex,
		NReduce:   mapTS.nReduce,
		NFiles:    mapTS.nFiles,
	}
}

func (TS *TaskStat) OutOfTime() bool {
	duration := time.Now().Sub(TS.beginTime)
	maxDuration := time.Duration(time.Second * 60)
	return duration > maxDuration
}

func (TS *TaskStat) SetNow() {
	TS.beginTime = time.Now()
}

func (TS *TaskStat) GetFileIndex() int {
	return TS.fileIndex
}

func (TS *TaskStat) GetPartIndex() int { return TS.partIndex }

// 任务队列

type TaskStatQueue struct {
	taskArray []TaskStatInterface
	mutex     sync.Mutex
}

func (queue *TaskStatQueue) lock() {
	//fmt.Println("queue lock")
	queue.mutex.Lock()
}

func (queue *TaskStatQueue) unlock() {
	//fmt.Println("queue unlock")
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

func (queue *TaskStatQueue) TimeOutQueue() []TaskStatInterface {
	outArray := make([]TaskStatInterface, 0)
	queue.lock()
	for taskIndex := 0; taskIndex < queue.size(); {
		taskStat := queue.taskArray[taskIndex]
		if taskStat.OutOfTime() {
			outArray = append(outArray, taskStat)
			queue.taskArray = append(queue.taskArray[:taskIndex], queue.taskArray[taskIndex+1:]...)
		} else {
			taskIndex++
		}
	}
	queue.unlock()
	return outArray
}

func (queue *TaskStatQueue) MoveAppend(rhs []TaskStatInterface) {
	queue.lock()
	queue.taskArray = append(queue.taskArray, rhs...)
	rhs = make([]TaskStatInterface, 0)
	queue.unlock()
}

func (queue *TaskStatQueue) RemoveTask(fileIndex int, partIndex int) {
	queue.lock()
	for index := 0; index < queue.size(); {
		task := queue.taskArray[index]
		if fileIndex == task.GetFileIndex() && partIndex == task.GetPartIndex() {
			queue.taskArray = append(queue.taskArray[:index], queue.taskArray[index+1:]...)
		} else {
			index++
		}
	}
	queue.unlock()
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

type Master struct {
	// Your definitions here.
	filenames []string

	mapTaskWaiting TaskStatQueue
	mapTaskRunning TaskStatQueue

	reduceTaskWaiting TaskStatQueue
	reduceTaskRunning TaskStatQueue

	isDone  bool
	nReduce int
}

func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) TaskDone(args *TaskInfo, reply *ExampleReply) error {
	switch args.State {
	case TaskMap:
		m.mapTaskRunning.RemoveTask(args.FileIndex, args.PartIndex)
		fmt.Printf("Map task on %vth file %v complete! Remaining running tasks:[%v]\n", args.FileIndex, args.FileName, m.mapTaskRunning.size())
		// 所有 map 任务完成，开始分配 reduce 任务
		if m.mapTaskWaiting.size() == 0 && m.mapTaskRunning.size() == 0 {
			m.distributeReduceTask()
		}
		break
	case TaskReduce:
		fmt.Printf("Reduce task on %vth part complete! Remaining running tasks:[%v]\n", args.PartIndex, m.reduceTaskRunning.size())
		m.reduceTaskRunning.RemoveTask(args.FileIndex, args.PartIndex)
		break
	default:
		panic("Unknown state")
	}
	return nil
}

func (m *Master) AskTask(args *ExampleArgs, reply *TaskInfo) error {
	// 查看任务是否完成
	if m.isDone {
		reply.State = TaskEnd
		return nil
	}

	// 是否有可用的reduce任务
	reduceTask := m.reduceTaskWaiting.pop()
	if reduceTask != nil {
		fmt.Printf("Distributing reduce task...\n")
		// 记录任务开始时间
		reduceTask.SetNow()
		m.reduceTaskRunning.push(reduceTask)
		*reply = reduceTask.GenerateTaskInfo()
		fmt.Printf("Distributing reduce task on part %vth file %v, Remaining waiting tasks:[%v]\n", reply.PartIndex, reply.FileName, m.reduceTaskWaiting.size())
		return nil
	}

	// 是否有可用的map任务
	mapTask := m.mapTaskWaiting.pop()
	if mapTask != nil {
		fmt.Printf("Distributing map task...\n")
		// 记录任务开始时间
		mapTask.SetNow()
		m.mapTaskRunning.push(mapTask)
		*reply = mapTask.GenerateTaskInfo()
		fmt.Printf("Distributing map task on %vth file %v, Remaining waiting tasks:[%v]\n", reply.FileIndex, reply.FileName, m.mapTaskWaiting.size())
		return nil
	}

	// should exit
	// 所有任务已分配
	if m.mapTaskRunning.size() > 0 || m.reduceTaskRunning.size() > 0 {
		fmt.Println("All tasks had been distributed")
		fmt.Printf("running map Task:[%v] running reduce Task:[%v]\n", m.mapTaskRunning.size(), m.reduceTaskRunning.size())
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
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	// fmt.Println(sockname)
	os.Remove(sockname)
	// For Unix networks, the address must be a file system path.

	l, e := net.Listen("unix", sockname)
	println("start listening")
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
	//if m.isDone {
	//	log.Println("asked whether i am done, replying yes...")
	//} else {
	//	log.Println("asked whether i am done, replying no...")
	//}
	return m.isDone
}

// 分发 Reduce 任务
func (m *Master) distributeReduceTask() {
	reduceTask := ReduceTaskStat{
		TaskStat{
			fileIndex: 0,
			partIndex: 0,
			nReduce:   m.nReduce,
			nFiles:    len(m.filenames),
		},
	}

	for reduceIndex := 0; reduceIndex < m.nReduce; reduceIndex++ {
		task := reduceTask
		task.partIndex = reduceIndex
		m.reduceTaskWaiting.push(&task)
	}
}

func (m *Master) collectOutOfTime() {
	for m.isDone == false {
		fmt.Println("collect OutOfTime Task...")
		time.Sleep(time.Duration(time.Second * 5))
		if m.reduceTaskRunning.size() > 0 {
			timeoutTasks := m.reduceTaskRunning.TimeOutQueue()
			if len(timeoutTasks) > 0 {
				m.reduceTaskWaiting.MoveAppend(timeoutTasks)
			}
		}
		if m.mapTaskRunning.size() > 0 {
			timeoutTasks := m.mapTaskRunning.TimeOutQueue()
			if len(timeoutTasks) > 0 {
				m.mapTaskWaiting.MoveAppend(timeoutTasks)
			}
		}
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func MakeMaster(files []string, nReduce int) *Master {

	// distribute map tasks
	mapArray := make([]TaskStatInterface, 0)
	for fileIndex, filename := range files {
		mapTask := MapTaskStat{
			TaskStat{
				fileName:  filename,
				fileIndex: fileIndex,
				partIndex: 0,
				nReduce:   nReduce,
				nFiles:    len(files),
			},
		}
		mapArray = append(mapArray, &mapTask)
	}
	m := Master{
		mapTaskWaiting: TaskStatQueue{taskArray: mapArray},
		isDone:         false,
		filenames:      files,
		nReduce:        nReduce}

	if _, err := os.Stat("mr-tmp"); os.IsNotExist(err) {
		err = os.Mkdir("mr-tmp", os.ModePerm)
		if err != nil {
			log.Fatalf("Create temp directory failed... Error: %v\n", err)
		}
	}

	go m.collectOutOfTime()

	m.server()
	return &m
}
