package main

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

func readCorpus() string {
	file, err := os.OpenFile("./corpus.txt", os.O_RDONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	bytes, err := io.ReadAll(file)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

func main() {
	text := readCorpus()
	chunkSize := len(text) / runtime.NumCPU()
	mapResult := make([]MapResult, 0)
	mtx := sync.Mutex{}
	startIdx, endIdx := 0, 0

	wg := sync.WaitGroup{}
	wg.Add(runtime.NumCPU())

	t1 := time.Now()
	for range runtime.NumCPU() {
		startIdx = endIdx
		endIdx = startIdx + chunkSize
		if endIdx > len(text) {
			endIdx = len(text)
		}

		go func(start, end int) {
			defer wg.Done()
			res := mapF(text[start:end])
			mtx.Lock()
			mapResult = append(mapResult, res...)
			mtx.Unlock()
		}(startIdx, endIdx)
	}

	wg.Wait()

	fmt.Println("finished doing map, took", time.Since(t1).Seconds(), "seconds")

	grouperResult := make([]GrouperResult, 0)
	chunkSize = len(mapResult) / runtime.NumCPU()
	startIdx, endIdx = 0, 0
	wg.Add(runtime.NumCPU())
	t1 = time.Now()
	for range runtime.NumCPU() {
		startIdx = endIdx
		endIdx = startIdx + chunkSize
		if endIdx > len(mapResult) {
			endIdx = len(mapResult)
		}

		go func(start, end int) {
			defer wg.Done()
			res := groupF(mapResult[start:end])
			mtx.Lock()
			grouperResult = append(grouperResult, res)
			mtx.Unlock()
		}(startIdx, endIdx)
	}

	wg.Wait()

	fmt.Println("finished doing group, took", time.Since(t1).Seconds(), "seconds")

	file, err := os.OpenFile("./mapres", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}

	t1 = time.Now()
	for word, count := range reduceF(grouperResult).reduced {
		fmt.Fprintf(file, "%s -> %d\n", word, count)
	}

	fmt.Println("successfully dumped contents to the file, took", time.Since(t1).Seconds(), "seconds")
}

type MapResult struct {
	word  string
	count int
}

func mapF(text string) []MapResult {
	words := strings.Fields(text)
	mvs := make([]MapResult, 0, len(words))
	for _, word := range words {
		mvs = append(mvs, MapResult{word, 1})
	}
	return mvs
}

type GrouperResult struct {
	groups map[string][]MapResult
}

func groupF(mv []MapResult) GrouperResult {
	groupResult := GrouperResult{make(map[string][]MapResult)}
	for _, i := range mv {
		groupResult.groups[i.word] = append(groupResult.groups[i.word], i)
	}
	return groupResult
}

type ReduceResult struct {
	reduced map[string]int
}

func reduceF(results []GrouperResult) ReduceResult {
	reduceResult := ReduceResult{make(map[string]int)}

	for _, result := range results {
		for word, groups := range result.groups {
			reduceResult.reduced[word] += len(groups)
		}
	}
	return reduceResult
}
