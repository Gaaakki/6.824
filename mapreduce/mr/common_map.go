package mr

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"sort"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doMap(task Task, mapf func(string, string) []KeyValue) {
	intermediateFiles := make([]*os.File, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		intermediateFileName := intermediateFileName(task.TaskNum, i)
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
		enc := json.NewEncoder(intermediateFiles[idx])
		err := enc.Encode(kv)
		if err != nil {
			log.Println("Error in encoding json")
		}
	}
	for i := 0; i < task.NReduce; i++ {
		intermediateFiles[i].Close()
	}
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
