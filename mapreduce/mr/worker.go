package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMap(task Task, mapf func(string, string) []KeyValue) {
	intermediateFiles := make([]*os.File, task.NReduce)
	for i:= 0; i < task.NReduce; i++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", task.TaskNum, i)
		intermediateFile, _ := os.Create(intermediateFileName)
		intermediateFiles[i] = intermediateFile
	}

	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()
	kva := mapf(task.Filename, string(content))
	sort.Sort(ByKey(kva))
	for _, kv := range kva {
		idx := ihash(kv.Key) % task.NReduce
		fmt.Fprintf(intermediateFiles[idx], "%v %v\n", kv.Key, kv.Value)
	}
	for i:= 0; i < task.NReduce; i++ {
		intermediateFiles[i].Close()
	}
}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	task := Task{}
	call("Master.AskTask", 1, &task)
	if task.Phase == "Wait" {
		time.Sleep(time.Second)
		Worker(mapf, reducef)
	} else if task.Phase == "Map" {
		doMap(task, mapf)
		call("Master.FinishMap", task.Filename, nil)
		Worker(mapf, reducef)
	} else if task.Phase == "Reduce" {
		fmt.Println(task.Phase, task.NReduce, task.TaskNum)
	}


}


//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
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
