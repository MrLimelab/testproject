package main

import (
	"context"
	"fmt"
	"sync"
	"tempProject/threadcontroller"
	"time"
)

func main()  {
	ctx, cancel := context.WithCancel(context.Background())
	totalThreads := 10
	threadTimeLimit := threadcontroller.ThreadTimeLimit{
		Threads: 5,
		Time:    time.Second,
	}

	controller := threadcontroller.NewController(totalThreads, threadTimeLimit)

	wg := &sync.WaitGroup{}

	go controller.Run(ctx, wg)
	commandChan := controller.CommandChannel()

	chunkSize := 10
	jobWg := &sync.WaitGroup{}
	for i:=0; i<10; i++ {
		rangeFrom := i * chunkSize + 1
		rangeTo := rangeFrom + chunkSize
		jobWg.Add(1)
		commandChan <- printNumbers(jobWg, rangeFrom, rangeTo)
	}

	jobWg.Wait()
	cancel()
	wg.Wait()
}

func printNumbers(wg *sync.WaitGroup, from int, to int) threadcontroller.Job{
	return func() {
		defer wg.Done()
		for i:=from; i<to; i++ {
			fmt.Println(i)
		}
	}
}


