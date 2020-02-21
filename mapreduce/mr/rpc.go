package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// Add your RPC definitions here.

type Task struct {
	Phase    string
	NMap   int
	NReduce  int
	TaskNum  int
	Filename string
}
