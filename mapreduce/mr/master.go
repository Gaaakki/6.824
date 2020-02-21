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
	nMap              int
	nReduce           int
	finishedMap       int
	finishedReduce    int
	mapTask           map[string]int
	reduceTask        map[int]bool
	inputFiles        []string
	intermediateFiles []int
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
		reply.TaskNum = m.mapTask[file]
		reply.Filename = file
	} else if m.finishedMap < m.nMap {
		reply.Phase = "Wait"
	} else if len(m.intermediateFiles) > 0 {
		reduceNum := m.intermediateFiles[0]
		m.intermediateFiles = m.intermediateFiles[1:]
		reply.Phase = "Reduce"
		reply.NMap = m.nMap
		reply.NReduce = m.nReduce
		reply.TaskNum = reduceNum
	} else if m.finishedReduce < m.nReduce {
		reply.Phase = "Wait"
	} else {
		reply.Phase = "Done"
	}
	return nil
}

func (m *Master) FinishMap(filename string, reply *Task) error {
	m.Lock()
	defer m.Unlock()
	m.mapTask[filename] = -1
	m.finishedMap++
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
	return m.finishedReduce == m.nReduce
}

//
// create a Master.
// main/mrmaster.go calls this function.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	m.nMap = len(files)
	m.nReduce = nReduce
	m.mapTask = make(map[string]int)
	for idx, file := range files {
		m.mapTask[file] = idx
		m.inputFiles = append(m.inputFiles, file)
	}
	for i:= 0; i < nReduce; i++ {
		m.intermediateFiles = append(m.intermediateFiles, i)
		m.reduceTask[i] = false
	}
	m.server()
	return &m
}
