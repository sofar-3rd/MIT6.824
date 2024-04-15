package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// 表示任务类型
const (
	TaskMap    = 0
	TaskReduce = 1
	TaskWait   = 2
	TaskEnd    = 3
)

type TaskInfo struct {
	/*
		Declared in const above
			0  map
			1  reduce
			2  wait
			3  end
	*/
	State int // 任务类型

	FileName  string // 文件名
	FileIndex int    // 文件序号 map 任务序号
	PartIndex int    // reduce 任务序号

	NReduce int // reduce 任务个数
	NFiles  int // 文件个数 map 任务个数
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	// s := "/var/tmp/824-mr-"
	s := "/home/lin/unixSock/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
