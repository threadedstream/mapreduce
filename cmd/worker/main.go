package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"log"

	"github.com/threadedstream/mapreduce/internal/mr"
	"github.com/threadedstream/mapreduce/internal/pkg/loader"
)
import "os"
import "fmt"

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	wrk := &mr.Worker{}

	mapFn, reduceFn := loader.LoadPlugin(os.Args[1])

	wrk.Map = mapFn
	wrk.Reduce = reduceFn

	var err error
	if wrk.ID, err = GetWorkerID(); err == nil {
		wrk.Do()
		return
	}

	log.Fatalf("failed to run worker!!!, reason: %s", err)
}

func GetWorkerID() (int, error) {
	args := mr.AssignIDArgs{}
	reply := mr.AssignIDReply{}

	if err := mr.Call("Coordinator.AssignID", &args, &reply); err != nil {
		return 0, err
	}

	return reply.ID, nil
}
