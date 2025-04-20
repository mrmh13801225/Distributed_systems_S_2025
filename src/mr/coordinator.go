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
func (c *Coordinator) AskForTask(req *MessageSend, reply *MessageReply) error {
	if req.MsgType != AskForTask {
		return BadMsgType
	}
	if !c.MapSuccess {

		c.muMap.Lock()

		count_map_success := 0
		for fileName, taskinfo := range c.MapTasks {
			alloc := false

			if taskinfo.Status == idle || taskinfo.Status == failed {
				alloc = true
			} else if taskinfo.Status == running {
				curTime := time.Now().Unix()
				if curTime-taskinfo.StartTime > 10 {
					taskinfo.StartTime = curTime
					alloc = true
				}
			} else {
				count_map_success++
			}

			if alloc {
				reply.MsgType = MapTaskAlloc
				reply.TaskName = fileName
				reply.NReduce = c.NReduce
				reply.TaskID = taskinfo.TaskId


				taskinfo.Status = running
				taskinfo.StartTime = time.Now().Unix()
				c.muMap.Unlock()
				return nil
			}
		}

		c.muMap.Unlock()

		if count_map_success < len(c.MapTasks) {
			reply.MsgType = Wait
			return nil
		} else {
			c.MapSuccess = true
		}
	}

	if !c.ReduceSuccess {
		c.muReduce.Lock()

		count_reduce_success := 0
		for idx, taskinfo := range c.ReduceTasks {
			alloc := false
			if taskinfo.Status == idle || taskinfo.Status == failed {
				alloc = true
			} else if taskinfo.Status == running {
				curTime := time.Now().Unix()
				if curTime-taskinfo.StartTime > 10 {
					taskinfo.StartTime = curTime
					alloc = true
				}
			} else {
				count_reduce_success++
			}

			if alloc {
				reply.MsgType = ReduceTaskAlloc
				reply.TaskID = idx


				taskinfo.Status = running
				taskinfo.StartTime = time.Now().Unix()

				c.muReduce.Unlock()
				return nil
			}
		}

		c.muReduce.Unlock()

		if count_reduce_success < len(c.ReduceTasks) {
			reply.MsgType = Wait
			return nil
		} else {
			c.ReduceSuccess = true
		}
	}

	reply.MsgType = Shutdown

	return nil
}

type NoticeResultStrategy interface {
	Handle(c *Coordinator, req *MessageSend) error
}

type MapSuccessStrategy struct{}

func (s *MapSuccessStrategy) Handle(c *Coordinator, req *MessageSend) error {
	c.muMap.Lock()
	defer c.muMap.Unlock()
	for _, v := range c.MapTasks {
		if v.TaskId == req.TaskID {
			v.Status = finished
			return nil
		}
	}
	return nil
}

type ReduceSuccessStrategy struct{}

func (s *ReduceSuccessStrategy) Handle(c *Coordinator, req *MessageSend) error {
	c.muReduce.Lock()
	defer c.muReduce.Unlock()
	if req.TaskID < len(c.ReduceTasks) {
		c.ReduceTasks[req.TaskID].Status = finished
	}
	return nil
}

type MapFailedStrategy struct{}

func (s *MapFailedStrategy) Handle(c *Coordinator, req *MessageSend) error {
	c.muMap.Lock()
	defer c.muMap.Unlock()
	for _, v := range c.MapTasks {
		if v.TaskId == req.TaskID && v.Status == running {
			v.Status = failed
			return nil
		}
	}
	return nil
}


type ReduceFailedStrategy struct{}

func (s *ReduceFailedStrategy) Handle(c *Coordinator, req *MessageSend) error {
	c.muReduce.Lock()
	defer c.muReduce.Unlock()
	if req.TaskID < len(c.ReduceTasks) && c.ReduceTasks[req.TaskID].Status == running {
		c.ReduceTasks[req.TaskID].Status = failed
	}
	return nil
}

var strategies = map[MsgType]NoticeResultStrategy{
	MapSuccess:    &MapSuccessStrategy{},
	ReduceSuccess: &ReduceSuccessStrategy{},
	MapFailed:     &MapFailedStrategy{},
	ReduceFailed:  &ReduceFailedStrategy{},
}

func (c *Coordinator) NoticeResult(req *MessageSend, reply *MessageReply) error {
	strategy, ok := strategies[req.MsgType]
	if !ok {
		return nil
	}
	return strategy.Handle(c, req)
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
	// Your code here.
	for _, taskinfo := range c.MapTasks {
		if taskinfo.Status != finished {
			return false
		}
	}

	for _, taskinfo := range c.ReduceTasks {
		if taskinfo.Status != finished {
			return false
		}
	}

	time.Sleep(time.Second * 3)

	return true
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
