package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var (
	errNoJobs     = errors.New("no jobs left")
	errNoWorkerID = errors.New("no id assigned to worker")
)

type Coordinator struct {
	jobs              chan string
	nReduce, workerID int
	taskMu, assignMu  *sync.Mutex
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.taskMu.Lock()
	defer c.taskMu.Unlock()
	if args.ID == nil {
		return errNoWorkerID
	}
	job := c.getAvailableJob()
	if job == "" {
		return errNoJobs
	}
	reply.File = job
	reply.Timeout = time.Second * 10
	reply.NReduce = 2
	return nil
}

func (c *Coordinator) AssignID(args *AssignIDArgs, reply *AssignIDReply) error {
	c.assignMu.Lock()
	defer c.assignMu.Unlock()
	reply.ID = c.workerID
	c.workerID++
	return nil
}

func (c *Coordinator) SignalFailure(args *SignalFailureArgs, reply *SignalFailureReply) error {
	c.jobs <- args.File
	return nil
}

func (c *Coordinator) getAvailableJob() string {
	if len(c.jobs) > 0 {
		return <-c.jobs
	}
	return ""
}

// start a thread that listens for RPCs from worker.go
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

// Done signals about coordinator's job completion
func (c *Coordinator) Done() bool {
	if len(c.jobs) > 0 {
		return false
	}
	return true
}

// MakeCoordinator creates Coordinator instance with supplied parameters
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fs := make(chan string, len(files))
	for _, f := range files {
		log.Printf("processing %s\n...", f)
		fs <- f
	}

	c := Coordinator{
		nReduce:  nReduce,
		jobs:     fs,
		taskMu:   new(sync.Mutex),
		assignMu: new(sync.Mutex),
		workerID: 1,
	}

	c.server()
	return &c
}
