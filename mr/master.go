package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

var maptasks chan string //Map任务的通道
var reducetasks chan int //Reduce任务的通道

type Master struct {
	// Your definitions here.
	FilesName          map[string]int //输入文件，key是文件名，value是文件状态
	MapTaskNum         int            //当前Map编号
	NReduce            int            //Reduce任务数：10
	LocForIntermediate [][]string     //中间文件存放路径
	ReduceTask         map[int]int    //Reduce任务，key是任务编号，value是任务状态
	MapFinished        bool           //Map是否完成
	ReduceFinished     bool           //Reduce是否完成
	RWLock             *sync.RWMutex  //锁
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) RPCHandler(args *MyArgs, reply *MyReply) error {
	msgType := args.MessageType
	switch msgType {
	case Msg_AskingTask: //发送任务，Map全部执行完之前不会发送Reduce
		select {
		case filename := <-maptasks:
			reply.Filename = filename
			reply.MapNum = m.MapTaskNum
			reply.NReduce = m.NReduce
			reply.TaskType = "map"

			m.RWLock.Lock()
			m.FilesName[filename] = Allocated
			m.MapTaskNum++
			m.RWLock.Unlock()
			go m.timerForWorker("map", filename)

			return nil

		case reduceNum := <-reducetasks:
			reply.TaskType = "reduce"
			reply.ReduceFileList = m.LocForIntermediate[reduceNum]
			reply.NReduce = m.NReduce
			reply.ReduceNum = reduceNum

			m.RWLock.Lock()
			m.ReduceTask[reduceNum] = Allocated
			m.RWLock.Unlock()
			go m.timerForWorker("reduce", strconv.Itoa(reduceNum))
			return nil
		}
	case Msg_MapFinished:
		m.RWLock.Lock()
		defer m.RWLock.Unlock()
		m.FilesName[args.MessageCnt] = Finished
	case Msg_ReduceFinished:
		index, _ := strconv.Atoi(args.MessageCnt)
		m.RWLock.Lock()
		defer m.RWLock.Unlock()
		m.ReduceTask[index] = Finished
	}
	return nil
}

//读取NReduceType字段获取Reduce任务编号，存放在相应位置
func (m *Master) MyInnerFileHandler(args *MyIntermediateFile, reply *MyReply) error {
	nReduceNUm := args.NReduceType
	filename := args.MessageCnt

	m.LocForIntermediate[nReduceNUm] = append(m.LocForIntermediate[nReduceNUm], filename)
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
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	maptasks = make(chan string, 5)
	reducetasks = make(chan int, 5)

	rpc.Register(m)
	rpc.HandleHTTP()

	go m.generateTask()

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
	ret = m.ReduceFinished

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//

const (
	UnAllocated = iota
	Allocated
	Finished
)

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.FilesName = make(map[string]int)
	m.MapTaskNum = 0
	m.NReduce = nReduce
	m.LocForIntermediate = make([][]string, m.NReduce)
	m.MapFinished = false
	m.ReduceFinished = false
	m.ReduceTask = make(map[int]int)
	m.RWLock = new(sync.RWMutex)
	for _, v := range files {
		m.FilesName[v] = UnAllocated
	}

	for i := 0; i < nReduce; i++ {
		m.ReduceTask[i] = UnAllocated
	}

	m.server()
	return &m
}

func checkAllMap(m *Master) bool {
	m.RWLock.RLock()
	defer m.RWLock.RUnlock()
	for _, v := range m.FilesName {
		if v != Finished {
			return false
		}
	}
	return true
}

func checkReduce(m *Master) bool {
	m.RWLock.RLock()
	defer m.RWLock.RUnlock()
	for _, v := range m.ReduceTask {
		if v != Finished {
			return false
		}
	}
	return true
}

func (m *Master) generateTask() {
	for k, v := range m.FilesName {
		if v == UnAllocated {
			maptasks <- k
		}
	}
	ok := false
	for !ok {
		ok = checkAllMap(m)
	}

	m.MapFinished = true

	for k, v := range m.ReduceTask {
		if v == UnAllocated {
			reducetasks <- k
		}
	}

	ok = false
	for !ok {
		ok = checkReduce(m)
	}
	m.ReduceFinished = true
}

func (m *Master) timerForWorker(taskType, identify string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if taskType == "map" {
				m.RWLock.Lock()
				m.FilesName[identify] = UnAllocated
				m.RWLock.Unlock()
				maptasks <- identify
			} else if taskType == "reduce" {
				index, _ := strconv.Atoi(identify)
				m.RWLock.Lock()
				m.ReduceTask[index] = UnAllocated
				m.RWLock.Unlock()
				reducetasks <- index
			}
			return
		default:
			if taskType == "map" {
				m.RWLock.RLock()
				if m.FilesName[identify] == Finished {
					m.RWLock.RUnlock()
					return
				} else {
					m.RWLock.RUnlock()
				}
			} else if taskType == "reduce" {
				index, _ := strconv.Atoi(identify)
				m.RWLock.RLock()
				if m.ReduceTask[index] == Finished {
					m.RWLock.RUnlock()
					return
				} else {
					m.RWLock.RUnlock()
				}
			}
		}
	}
}
