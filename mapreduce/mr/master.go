package mr

import (
	"sync"
	"time"
)


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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	// Your code here.
	return m.finishedReduce == m.nReduce
}

func (m *Master) checkMapFinished(file string) {
	time.Sleep(time.Second * 10)
	m.Lock()
	defer m.Unlock()
	if m.mapTask[file] != -1 {
		m.inputFiles = append(m.inputFiles, file)
	}
}

func (m *Master) checkReduceFinished(reduceNum int) {
	time.Sleep(time.Second * 10)
	m.Lock()
	defer m.Unlock()
	if !m.reduceTask[reduceNum] {
		m.intermediateFiles = append(m.intermediateFiles, reduceNum)
	}
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
	m.reduceTask = make(map[int]bool)
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
