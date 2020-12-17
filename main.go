package main

import (
	"errors"
	"fmt"
	"github.com/rahul476/alivecor/TaskQueue"
	"github.com/sirupsen/logrus"
	"math/rand"
	"strconv"
	"time"
)

const TimeOut = 2 * time.Second


// Check the status of all the task in the map
// and process it accordingly
func Cleaner(tq *TaskQueue.TaskQueue) {
	var err error
	for _, task := range tq.Tasks {
		switch task.Status {
		case TaskQueue.Processing:
			// Task in-progress
			// Can remove the task which is in process for more than x secs/minutes
		case TaskQueue.Completed:
			// remove from the queue
			err = ProcessCompletedTask(tq, task.Id)
		case TaskQueue.Failed:
			// remove from the queue and retry if not timeout
			err = ProcessFailedTask(tq, task.Id)
		case TaskQueue.Timeout:
			// remove from the queue and log
			err = ProcessTimedOutTask(tq, task.Id)
		}
		if err != nil {
			logrus.Errorln("Error : ", err)
		}
	}

}

// Remove the task from map
func ProcessCompletedTask(tq *TaskQueue.TaskQueue, id string) error {
	_, err := tq.RemoveTask(id)
	if err != nil {
		return fmt.Errorf("processCompletedTask: %w", err)
	}
	return nil
}

// Remove the task from map
// if its in queue for more than x time remove it and log
// else retry the task
func ProcessFailedTask(tq *TaskQueue.TaskQueue, id string) error {
	task, err := tq.RemoveTask(id)
	if err != nil {
		return fmt.Errorf("processFailedTask: %w", err)
	}
	// Check the start processing time (if )
	if task.CreationTime.Add(TimeOut).Before(time.Now()) { // dont retry and log
		logrus.Infoln("failed task timeout : ", id)
		return nil
	} else {
		logrus.Infoln("Retrying : ", id)
		task.Status = TaskQueue.Retry
		err := tq.AddNewTask(task)
		if err != nil {
			return fmt.Errorf("processFailedTask: %w", err)
		}
		return nil
	}
}

// Remove the task from map and log
func ProcessTimedOutTask(tq *TaskQueue.TaskQueue, id string) error {
	_, err := tq.RemoveTask(id)
	if err != nil {
		return fmt.Errorf("processTimedOutTask: %w", err)
	}
	logrus.Warnln("task timed out, taskId : ", id)
	return nil
}



// Pull data from the queue channel
// and process the task
func Executor(tq *TaskQueue.TaskQueue) {
	logrus.Infoln("Worker started")
	for taskId := range tq.Queue {
		err := tq.UpdateStatus(taskId, TaskQueue.Processing)
		if err != nil {
			logrus.Errorln("worker error updating the status to processing, Error ", err)
		}
		err = ProcessTask(tq, taskId)
		if err != nil {
			logrus.Errorln("worker error processing the Task, Error : ", err)
		}
	}
}

// Process the task
// For testing purpose: fail the task with probability of 1/3
func ProcessTask(tq *TaskQueue.TaskQueue, taskId string) error {
	logrus.Infoln("Processing task : ", taskId)
	// failing tasks with probability of 1/3
	randomInt := rand.Intn(3)
	if randomInt == 2 {
		logrus.Infoln("failed : ", taskId)
		err := tq.UpdateStatus(taskId, TaskQueue.Failed)
		if err != nil {
			return errors.New("process task error updating the status to failed, Error : " + err.Error())
		}
		return nil
	}

	err := tq.UpdateStatus(taskId, TaskQueue.Completed)
	if err != nil {
		return errors.New("process task error updating the status to completed, Error : " + err.Error())
	}
	logrus.Infoln("Completed task : ", taskId)
	return nil
}

func main() {
	taskQueue := TaskQueue.TaskQueue{}
	taskQueue.Init(50)

	// start clean up go-routine
	go func() {
		for {
			Cleaner(&taskQueue)
			time.Sleep(1 * time.Second)
		}
	}()

	// start worker go-routine
	go func() {
		Executor(&taskQueue)
	}()

	// start producer
	// can run this in go routine to push new task in the queue (just need to call AddNewTask function)
	for i := 0; i < 10; i++ {
		task := TaskQueue.Task{
			Id:           strconv.Itoa(i),
			Status:       TaskQueue.Untouched,
			CreationTime: time.Now(),
			TaskData:     string(i),
		}
		err := taskQueue.AddNewTask(task)
		if err != nil {
			logrus.Errorln("Error adding new task, Err : ", err)
		}

		randomInt := rand.Intn(1)
		sleepTime := time.Duration(int64(randomInt)) * time.Second
		time.Sleep(sleepTime)
	}

	select {}

}
