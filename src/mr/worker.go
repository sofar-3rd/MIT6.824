package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
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

func (BK ByKey) Len() int           { return len(BK) }
func (BK ByKey) Swap(i, j int)      { BK[i], BK[j] = BK[j], BK[i] }
func (BK ByKey) Less(i, j int) bool { return BK[i].Key < BK[j].Key }

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

func Worker(
	mapFunc func(string, string) []KeyValue,
	reduceFunc func(string, []string) string) {

	for {
		fmt.Printf("AskTask...\n")
		taskInfo := CallAskTask()
		fmt.Printf("Got %v Task\n", taskInfo.State)
		switch taskInfo.State {
		case TaskMap:
			workerMap(mapFunc, taskInfo)
			break
		case TaskReduce:
			workReduce(reduceFunc, taskInfo)
			break
		case TaskWait:
			time.Sleep(time.Duration(time.Second * 5))
			break
		case TaskEnd:
			fmt.Println("Master all tasks complete. Nothing to do...")

		default:
			panic("Invalid Task state received by worker")
		}
	}

}

func workReduce(reduceFunc func(string, []string) string, info *TaskInfo) {
	fmt.Printf("Got assigned reduce task on part %v\n", info.PartIndex)
	outName := "mr-out-" + strconv.Itoa(info.PartIndex)

	// 读取所有 mr-[fileIndex]-[partIndex] fileIndex 为每个 map 任务编号
	inputFileNamePrefix := "mr-tmp/mr-"
	inputFileNameSuffix := "-" + strconv.Itoa(info.PartIndex)

	intermediate := []KeyValue{}
	for index := 0; index < info.NFiles; index++ {
		inputFileName := inputFileNamePrefix + strconv.Itoa(index) + inputFileNameSuffix
		file, err := os.Open(inputFileName)
		if err != nil {
			fmt.Printf("Open intermediate file %v failed: %v\n", inputFileName, err)
			panic("Open file error")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	ofile, err := ioutil.TempFile("mr-tmp", "mr-*")
	if err != nil {
		fmt.Printf("Create output file %v failed: %v\n", outName, err)
		panic("Create file error")
	}
	//fmt.Printf("%v\n", intermediate)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reduceFunc(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Rename(filepath.Join(ofile.Name()), outName)
	ofile.Close()

	// acknowledge master
	CallTaskDone(info)
}

func workerMap(mapFunc func(string, string) []KeyValue, info *TaskInfo) {
	// 读文件
	var intermediate []KeyValue
	file, err := os.Open(info.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", info.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", info.FileName)
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("cannot close %v", info.FileName)
	}

	// 使用 mapFunc 计算键值对
	kva := mapFunc(info.FileName, string(content))
	intermediate = append(intermediate, kva...)

	nReduce := info.NReduce
	outFiles := make([]*os.File, nReduce)
	fileEncoder := make([]*json.Encoder, nReduce)

	outPrefix := "mr-tmp/mr-"
	outPrefix += strconv.Itoa(info.FileIndex)
	outPrefix += "-"

	for outIndex := 0; outIndex < nReduce; outIndex++ {
		outFiles[outIndex], _ = ioutil.TempFile("mr-tmp", "mr-tmp-*")
		fileEncoder[outIndex] = json.NewEncoder(outFiles[outIndex])
	}

	// 通过哈希将相同 key 的键值对存放到相同编号的 nReduce 文件中
	for _, kv := range intermediate {
		// 通过哈希 reduce 文件编号
		outIndex := ihash(kv.Key) % nReduce

		// 文件及解码器
		file = outFiles[outIndex]
		enc := fileEncoder[outIndex]

		// json编码 写入文件
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Printf("File %v Key %v Value %v Error %v\n", info.FileName, kv.Key, kv.Value, err)
			panic("Json encode failed")
		}
	}

	for outIndex, file := range outFiles {
		outName := outPrefix + strconv.Itoa(outIndex)
		oldPath := filepath.Join(file.Name())
		os.Rename(oldPath, outName)
		file.Close()
	}

	CallTaskDone(info)
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

// worker 向 master 申请任务

func CallAskTask() *TaskInfo {
	args := ExampleArgs{}
	reply := TaskInfo{}
	call("Master.AskTask", &args, &reply)
	return &reply
}

// worker 通知 master 任务完成

func CallTaskDone(taskInfo *TaskInfo) {
	reply := ExampleReply{}
	call("Master.TaskDone", taskInfo, &reply)
}
