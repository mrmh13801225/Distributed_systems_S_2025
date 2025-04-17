package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type taskStatus int

const (
	idle     taskStatus = iota 
	running                   
	finished                  
	failed                     
)

type MapTaskInfo struct {
	TaskId    int       
	Status    taskStatus 
	StartTime int64  
}

type ReduceTaskInfo struct {
	Status    taskStatus 
	StartTime int64      
}

type Coordinator struct {
	NReduce       int                     
	MapTasks      map[string]*MapTaskInfo 
	MapSuccess    bool                   
	muMap         sync.Mutex              
	ReduceTasks   []*ReduceTaskInfo       
	ReduceSuccess bool                   
	muReduce      sync.Mutex              
}


func (c *Coordinator) initTask(files []string) {
	for idx, fileName := range files {
		c.MapTasks[fileName] = &MapTaskInfo{
			TaskId: idx,
			Status: idle,
		}
	}
	for idx := range c.ReduceTasks {
		c.ReduceTasks[idx] = &ReduceTaskInfo{
			Status: idle,
		}
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
        NReduce:     nReduce,
        MapTasks:    make(map[string]*MapTaskInfo),
        ReduceTasks: make([]*ReduceTaskInfo, nReduce),
    }

	c.initTask(files)

	c.server()
	return &c
}
