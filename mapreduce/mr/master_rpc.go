package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

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
		go m.checkMapFinished(file)
	} else if m.finishedMap < m.nMap {
		reply.Phase = "Wait"
	} else if len(m.intermediateFiles) > 0 {
		reduceNum := m.intermediateFiles[0]
		m.intermediateFiles = m.intermediateFiles[1:]
		reply.Phase = "Reduce"
		reply.NMap = m.nMap
		reply.NReduce = m.nReduce
		reply.TaskNum = reduceNum
		go m.checkReduceFinished(reduceNum)
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

func (m *Master) FinishReduce(taskNum int, reply *Task) error {
	for i:= 0; i < m.nMap; i++ {
		intermediateFileName := intermediateFileName(i, taskNum)
		os.Remove(intermediateFileName)
	}
	m.Lock()
	defer m.Unlock()
	m.reduceTask[taskNum] = true
	m.finishedReduce++
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
