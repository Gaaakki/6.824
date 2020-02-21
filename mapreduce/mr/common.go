package mr

import (
	"fmt"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

func intermediateFileName(taskNum, reduceNum int) string {
	return fmt.Sprintf("mr-%d-%d", taskNum, reduceNum)
}

func mergeName(reduceNum int) string {
	return fmt.Sprintf("mr-out-%d", reduceNum)
}
