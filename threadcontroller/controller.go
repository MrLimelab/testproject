package threadcontroller

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

/*
Controller represents an object with ability to control execution threads
*/
type Controller struct {
	//main channel of receiving jobs
	command chan Job

	//channel to receive jobs by workers in pool
	commandMuxed chan Job

	workersAmount int

	executedThreadTimeLimit  uint64
	threadsExecuted          uint64
	executedThreadsResetTime time.Duration
}

/*
Job type to be passed to channel will be executed by workers
any parameters that needed inside Job should be passed via closures
*/
type Job = func()

/*
ThreadTimeLimit parameter to be passed for NewController constructor
Threads contains maximum operations that can be executed in a Time
0 time means no limit
*/
type ThreadTimeLimit struct {
	Threads uint64
	Time    time.Duration
}

/*
NewController constructor for thread controller returns link to created controller
thread execution should be started with run command
totalThreads number of worker init in worker pool
threadLimit limit of job execution per time, 0 Time means no limit
*/
func NewController(
	totalThreads int,
	threadLimit ThreadTimeLimit) *Controller {
	return &Controller{
		workersAmount:            totalThreads,
		executedThreadTimeLimit:  threadLimit.Threads,
		executedThreadsResetTime: threadLimit.Time,
		command:                  make(chan Job, totalThreads),
		commandMuxed:             make(chan Job, totalThreads),
	}
}

/*
CommandChannel returns a channel, read by separate thread to execute Job
*/
func (c *Controller) CommandChannel() chan Job {
	return c.command
}

/*
Run init and start worker pool for thread controller
*/
func (c *Controller) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	wgController := &sync.WaitGroup{}

	wgController.Add(1)
	c.initWorkerPool(ctx, wgController)

	//no time limit if executedThreadsResetTime is 0
	if c.executedThreadsResetTime != 0 {
		wgController.Add(1)
		go c.executedThreadsCounterReset(ctx, wgController)
	}


	wgController.Add(1)
	go c.commandChannelReader(ctx, wgController)

	wgController.Wait()
}

func (c *Controller) initWorkerPool(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < c.workersAmount; i++ {
		wg.Add(1)
		go c.worker(ctx, wg)
	}
}

func (c *Controller) worker(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-c.commandMuxed:
			if job != nil {
				job()
			}
		}
	}
}

func (c *Controller) executedThreadsCounterReset(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(c.executedThreadsResetTime)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.threadLimitReset()
		}
	}
}

func (c *Controller) threadLimitReset() {
	atomic.SwapUint64(&c.threadsExecuted, uint64(0))
}

func (c *Controller) commandChannelReader(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-c.command:

			if c.threadLimitExceeded() {
				c.waitForThreadTimeLimitReset(ctx)
			}

			c.increaseExecutedThreadCounter()

			c.commandMuxed <- cmd

			if c.threadLimitExceeded() {
				c.waitForThreadTimeLimitReset(ctx)
			}
		}
	}
}

func (c *Controller) threadLimitExceeded() bool {
	threadsExecuted := atomic.LoadUint64(&c.threadsExecuted)
	if c.executedThreadsResetTime == 0 || threadsExecuted < c.executedThreadTimeLimit {
		return false
	}
	return true
}

func (c *Controller) waitForThreadTimeLimitReset(ctx context.Context) {
	for {
		if c.threadLimitExceeded() {
			select {
			case <-time.After(c.executedThreadsResetTime/4):
				continue
			case <-ctx.Done():
				return
			}
		} else {
			break
		}
	}
}

func (c *Controller) increaseExecutedThreadCounter() {
	if c.executedThreadsResetTime == 0 {
		return
	}
	atomic.AddUint64(&c.threadsExecuted, 1)
}
