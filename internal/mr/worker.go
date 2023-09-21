package mr

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// KeyValue is a lingo-franca of all mr apps
type KeyValue struct {
	Key   string
	Value string
}

type MapFn = func(string, string) []KeyValue
type ReduceFn = func(string, []string) string

type Worker struct {
	ID     int
	Map    MapFn
	Reduce ReduceFn
	Merge  bool
}

func (wrk *Worker) Do() {
	wakeUpTime := time.Second * 1
	ticker := time.NewTicker(wakeUpTime)
	errCount := 5
outer:
	for {
		select {
		case <-ticker.C:
			task, err := wrk.GetTask()
			if err != nil {
				// can't opt for a strict equality as error returned by rpc client is not the same returned by a server
				if err.Error() == errNoJobs.Error() {
					log.Println("done, no jobs left")
					break outer
				}
				log.Printf("unexpected error: %s\n", err)
				if errCount == 0 {
					break outer
				}
				errCount--
				continue outer
			}

			var payload map[string][]string
			if payload, err = wrk.doMap(task); err != nil {
				_, err = wrk.SignalFailure(task.File)
				if err != nil {
					log.Println("failed to SignalFailure(), reason: ", err)
				}
			}
			// performing reduce
			filePrefix, files := wrk.doReduce(payload, task)

			if err = wrk.doMerge(files, fmt.Sprintf("mr-out-%s-%d", filePrefix, wrk.ID)); err != nil {
				log.Println("failed to merge files, reason: ", err)
			}
		}
	}

}

func (wrk *Worker) doMap(task *GetTaskReply) (map[string][]string, error) {
	cnts, err := open(task.File)
	if err != nil {
		return nil, err
	}
	kvs := wrk.Map("", *cnts)
	sort.Sort(ByKey(kvs))

	payload := make(map[string][]string)
	var i int
	for i < len(kvs) {
		var vs []string
		j := i + 1
		for j < len(kvs) && kvs[i].Key == kvs[j].Key {
			j++
		}
		for k := i; k < j; k++ {
			vs = append(vs, kvs[k].Value)
		}

		payload[kvs[i].Key] = append(payload[kvs[i].Key], vs...)
		i = j
	}
	return payload, nil
}

func (wrk *Worker) doReduce(payload map[string][]string, task *GetTaskReply) (filePrefix string, files []string) {
	fileIdx, idx := 1, 0
	parts := strings.Split(task.File, "/")
	filePrefix = strings.Split(parts[len(parts)-1], ".")[0]

	workSize := len(payload) / task.NReduce

	files = append(files, fmt.Sprintf("mr-out-%s-%d-%d", filePrefix, wrk.ID, fileIdx))
	file, _ := os.Create(files[len(files)-1])
	for k, v := range payload {
		if workSize >= len(payload) {
			workSize = len(payload)
		}
		fmt.Fprintf(file, "%v %v\n", k, wrk.Reduce(k, v))
		if idx >= workSize-1 {
			workSize *= 2
			fileIdx += 1

			// recreate a file
			file.Close()
			files = append(files, fmt.Sprintf("mr-out-%s-%d-%d", filePrefix, wrk.ID, fileIdx))
			file, _ = os.Create(files[len(files)-1])
		}
		idx++
	}

	return filePrefix, files
}

func (wrk *Worker) doMerge(files []string, outName string) error {
	outf, _ := os.Create(outName)

	for _, file := range files {
		contents, err := open(file)
		if err != nil {
			return err
		}
		fmt.Fprint(outf, *contents)
	}
	return nil
}

func (wrk *Worker) GetTask() (*GetTaskReply, error) {
	args := GetTaskArgs{
		ID: &wrk.ID,
	}
	reply := GetTaskReply{}

	if err := Call("Coordinator.GetTask", &args, &reply); err != nil {
		return nil, err
	}

	return &reply, nil
}

func (wrk *Worker) SignalFailure(job string) (*SignalFailureReply, error) {
	args := SignalFailureArgs{
		File: job,
	}
	reply := SignalFailureReply{}

	if err := Call("Coordinator.SignalFailure", &args, &reply); err != nil {
		return nil, err
	}

	return &reply, nil
}

func open(filename string) (*string, error) {
	file, err := os.OpenFile(filename, os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	bs, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	return &[]string{string(bs)}[0], nil
}
