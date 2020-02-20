package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	sync.Mutex
	nReduce           int
	mapTaskNum        map[string]int
	inputFiles        []string
	dealingFiles      []string
	intermediateFiles []string
	doneChannel       chan struct{}
}

type Task struct {
	Phase       string
	TaskNum     int
	NReduce     int
	DoneChannel chan struct{}
	Filename    string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) AskTask(args int, reply *Task) error {
	m.Lock()
	defer m.Unlock()
	if len(m.inputFiles) > 0 {
		file := m.inputFiles[0]
		m.inputFiles = m.inputFiles[1:]
		reply.Phase = "Map"
		reply.NReduce = m.nReduce
		reply.TaskNum = m.mapTaskNum[file]
		reply.DoneChannel = make(chan struct{})
		reply.Filename = file
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
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

	// Your code here.
	select {
	case <-m.doneChannel:
		return true
	default:
		return false
	}

}

//
// create a Master.
// main/mrmaster.go calls this function.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	m.nReduce = nReduce
	m.mapTaskNum = make(map[string]int)
	m.doneChannel = make(chan struct{})
	for idx, file := range files {
		m.mapTaskNum[file] = idx
		m.inputFiles = append(m.inputFiles, file)
	}
	m.server()
	return &m
}
