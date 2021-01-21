package threadcontroller

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/suite"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type ControllerSuite struct {
	suite.Suite
}

func TestManager(t *testing.T) {
	suite.Run(t, new(ControllerSuite))
}


type managerTestEnvironment struct {
	context context.Context
	cancel  context.CancelFunc
	controller *Controller
	suite   *ControllerSuite
}

func (suite *ControllerSuite) setupEnv() *managerTestEnvironment {
	testEnv := managerTestEnvironment{}

	testEnv.suite = suite

	testEnv.context, testEnv.cancel = context.WithCancel(context.Background())

	return &testEnv
}


func (suite *ControllerSuite) TestControllerNoExecutionTimeLimit() {
	testEnv := suite.setupEnv()

	numberOfThreads := 10
	threadTimeLimit := ThreadTimeLimit{
		Threads: 0,
		Time:    0,
	}
	testEnv.controller = NewController(numberOfThreads, threadTimeLimit)



	var jobDoneCounter uint64 = 0
	jobDoneWg := &sync.WaitGroup{}
	jobSleepTime := 1 * time.Millisecond

	simultaneousJobRunning := int64(0)
	dummyJob := func(){
		jobsRunning := atomic.AddInt64(&simultaneousJobRunning, 1)
		if jobsRunning > int64(numberOfThreads) {
			panic("total running jobs started more than thread limit")
		}

		time.Sleep(jobSleepTime)
		atomic.AddUint64(&jobDoneCounter, 1)
		atomic.AddInt64(&simultaneousJobRunning, -1)
		jobDoneWg.Done()
	}


	numberOfJobsExecuted := 100
	jobDoneWg.Add(numberOfJobsExecuted)
	commandCh := testEnv.controller.CommandChannel()
	go func() {
		for i := 0; i<numberOfJobsExecuted; i++ {
			commandCh <- dummyJob
		}
	}()

	controllerWg := &sync.WaitGroup{}
	startTime := time.Now()
	go testEnv.controller.Run(testEnv.context, controllerWg)
	jobDoneWg.Wait()

	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)
	totalTime := jobSleepTime * time.Duration(numberOfJobsExecuted)

	suite.LessOrEqual(elapsedTime, totalTime)
	suite.Equal(uint64(numberOfJobsExecuted), atomic.LoadUint64(&jobDoneCounter))
}

func (suite *ControllerSuite) TestControllerWithExecutionTimeLimit() {
	testEnv := suite.setupEnv()

	numberOfThreads := 10
	threadTimeLimit := ThreadTimeLimit{
		Threads: 5,
		Time:    10 * time.Millisecond,
	}
	testEnv.controller = NewController(numberOfThreads, threadTimeLimit)


	var jobDoneCounter uint64 = 0
	jobDoneWg := &sync.WaitGroup{}
	//job sleepTime chosen less than threadTimeLimit.Time to create time overflow of jobs running
	//this means that only 5 jobs can be running simultaneously
	jobSleepTime := 1 * time.Millisecond

	simultaneousJobRunning := int64(0)
	dummyJob := func(){
		jobsRunning := atomic.AddInt64(&simultaneousJobRunning, 1)
		if jobsRunning > int64(threadTimeLimit.Threads) {
			text := fmt.Sprintf("total running jobs started more than thread limit\ntotal jobsRunning [%d] threadTimeLimit.Threads: [%d] ", jobsRunning, threadTimeLimit.Threads)
			panic(text)
		}

		time.Sleep(jobSleepTime)
		atomic.AddUint64(&jobDoneCounter, 1)
		atomic.AddInt64(&simultaneousJobRunning, -1)
		jobDoneWg.Done()
	}


	numberOfJobsExecuted := 100
	jobDoneWg.Add(numberOfJobsExecuted)
	commandCh := testEnv.controller.CommandChannel()
	go func() {
		for i := 0; i<numberOfJobsExecuted; i++ {
			commandCh <- dummyJob
		}
	}()

	controllerWg := &sync.WaitGroup{}
	startTime := time.Now()
	go testEnv.controller.Run(testEnv.context, controllerWg)
	jobDoneWg.Wait()

	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)
	totalTime := time.Duration(uint64(numberOfJobsExecuted) / threadTimeLimit.Threads) * threadTimeLimit.Time

	suite.checkTimeDurationWithinPercent(totalTime, elapsedTime, 10)
	suite.Equal(uint64(numberOfJobsExecuted), atomic.LoadUint64(&jobDoneCounter))
}

func (suite *ControllerSuite) checkTimeDurationWithinPercent(expected time.Duration, received time.Duration, percent int) {
		timeDelta := time.Duration((expected.Nanoseconds() / 100) * int64(percent))
		var absTimeDiff time.Duration
		if expected > received {
			absTimeDiff = expected - received
		} else {
			absTimeDiff = received - expected
		}
		suite.GreaterOrEqual(timeDelta, absTimeDiff)
}


func (suite *ControllerSuite) TestControllerContextDoneCheck() {
	testEnv := suite.setupEnv()

	numberOfThreads := 10
	threadTimeLimit := ThreadTimeLimit{
		Threads: 11,
		Time:    time.Second,
	}
	testEnv.controller = NewController(numberOfThreads, threadTimeLimit)

	var jobDoneCounter uint64 = 0
	jobDoneWg := &sync.WaitGroup{}
	jobUnlock := make(chan struct{})
	jobStarted := make(chan struct{})
	dummyJob := func(){
		jobStarted <- struct {}{}
		<- jobUnlock
		atomic.AddUint64(&jobDoneCounter, 1)
		jobDoneWg.Done()
	}


	jobDoneWg.Add(1)
	commandCh := testEnv.controller.CommandChannel()
	go func() {
			commandCh <- dummyJob
	}()

	controllerWg := &sync.WaitGroup{}
	controllerWg.Add(1)
	go testEnv.controller.Run(testEnv.context, controllerWg)

	<-jobStarted

	testEnv.cancel()
	startTime := time.Now()

	sleepTime := 100 * time.Millisecond
	time.Sleep(sleepTime)
	jobUnlock <- struct{}{}

	endTime := time.Now()

	elapsedTime := endTime.Sub(startTime)

	controllerWg.Wait()
	jobDoneWg.Wait()

	suite.checkTimeDurationWithinPercent(sleepTime, elapsedTime, 10)
	suite.Equal(uint64(1), jobDoneCounter)

}
