package mr

import (
	"fmt"
	"time"
)
import "log"
import "net/rpc"

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
	} else if task.Phase == "Map" {
		doMap(task, mapf)
		call("Master.FinishMap", task.Filename, nil)
	} else if task.Phase == "Reduce" {
		doReduce(task, reducef)
		call("Master.FinishReduce", task.TaskNum, nil)
	} else {
		return
	}
	Worker(mapf, reducef)
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
