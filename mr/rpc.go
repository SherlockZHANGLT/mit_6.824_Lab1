package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
const (
	Msg_AskingTask        = iota //寻求任务
	MsgForLocIntermediate        //发送中间文件的位置
	Msg_MapFinished              //完成Map任务
	Msg_ReduceFinished           //完成Reduce任务
)

//接收消息
type MyArgs struct {
	MessageType int
	MessageCnt  string
}

type MyIntermediateFile struct {
	MessageType int
	MessageCnt  string
	NReduceType int
}

//回复消息
type MyReply struct {
	Filename       string
	MapNum         int //Map任务编号
	ReduceNum      int //Reduce任务编号
	NReduce        int
	TaskType       string
	ReduceFileList []string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
