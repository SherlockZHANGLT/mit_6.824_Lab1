package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//Sort
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Your worker implementation here.
	for true {
		reply := CallForTask(Msg_AskingTask, "")
		if reply.TaskType == "" {
			break
		}
		switch reply.TaskType {
		case "map":
			Map(&reply, mapf)
		case "reduce":
			Reduce(&reply, reducef)
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

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

	if err.Error() != "unexpected EOF" {
		fmt.Println("error: ", err)
	}
	return false
}

func CallForTask(msgType int, msgCnt string) MyReply {
	args := MyArgs{}
	args.MessageType = msgType
	args.MessageCnt = msgCnt

	reply := MyReply{}

	res := call("Master.RPCHandler", &args, &reply)
	if !res {
		//  fmt.Printf("reply.type %v\n",reply.TaskType)
	}
	return reply
}

func SendIntermediateLoc(msgType int, msgCnt string, nReduceType int) MyReply {
	args := MyIntermediateFile{}
	args.MessageType = msgType
	args.MessageCnt = msgCnt
	args.NReduceType = nReduceType

	repley := MyReply{}

	res := call("Master.MyInnerFileHandler", &args, &repley)
	if !res {
		fmt.Println("error sending intermediate files' location")
	}
	return repley
}

func Partition(kva []KeyValue, nReduce int) [][]KeyValue {
	kvas := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		v := ihash(kv.Key) % nReduce
		kvas[v] = append(kvas[v], kv)
	}
	return kvas
}

func WriteToJSON(intermediate []KeyValue, mapTaskNum, reduceTaskNUm int) string {
	filename := "mr-" + strconv.Itoa(mapTaskNum) + "-" + strconv.Itoa(reduceTaskNUm)
	jfile, _ := os.Create(filename)

	enc := json.NewEncoder(jfile)
	for _, kv := range intermediate {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("error: ", err)
		}
	}
	return filename
}

func Map(reply *MyReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(reply.Filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}

	kva := mapf(reply.Filename, string(content))
	kvas := Partition(kva, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		filename := WriteToJSON(kvas[i], reply.MapNum, i)
		_ = SendIntermediateLoc(MsgForLocIntermediate, filename, i)
	}
	_ = CallForTask(Msg_MapFinished, reply.Filename)
}

func Reduce(reply *MyReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for _, v := range reply.ReduceFileList {
		file, err := os.Open(v)
		defer file.Close()
		if err != nil {
			log.Fatalf("cannot open %v", v)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(reply.ReduceNum)
	ofile, _ := os.Create(oname)

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
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	_ = CallForTask(Msg_ReduceFinished, strconv.Itoa(reply.ReduceNum))
}

func WriteToReduceOutput(key, values string, nReduce int) {
	filename := "mr-out-" + strconv.Itoa(nReduce)
	ofile, err := os.Open(filename)
	if err != nil {
		fmt.Println("no such file")
		ofile, _ = os.Create(filename)
	}

	fmt.Fprintf(ofile, "%v %v\n", key, values)
}
