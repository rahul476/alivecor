package main

import (
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

const (
	Untouched  = "untouched"
	Retry      = "retry"
	Processing = "processing"
	Completed  = "completed"
	Failed     = "failed"
	Timeout    = "timeout"
)

const TimeOut = 1 * time.Second

type Task struct {
	Id           string
	Status       string    // untouched, completed, failed, timeout
	CreationTime time.Time // when was the task created
	TaskData     string    // field containing data about the task
}

type TaskQueue struct {
	lock  sync.Mutex      // use to lock the Task map
	Tasks map[string]Task // store the task id and task details
	Queue chan string     // producer pushes the data to queue channel and consumer read from queue channel
}

// Global TasK Queue object
var taskQueue TaskQueue

// Initialise the task map and queue
func (tq *TaskQueue) Init() {
	tq.Tasks = make(map[string]Task, 0)
	tq.Queue = make(chan string, 5)
}

// Add new task to the map and push to the queue channel
func (tq *TaskQueue) AddNewTask(task Task) error {
	tq.lock.Lock()
	if _, ok := tq.Tasks[task.Id]; ok {
		tq.lock.Unlock()
		return errors.New("already present, taskId : " + task.Id) // TODO : use string concatination
	}
	tq.Tasks[task.Id] = task
	tq.lock.Unlock()
	producer(tq.Queue, task.Id)
	return nil
}

// Remove the task from the map
func (tq *TaskQueue) RemoveTask(id string) (Task, error) {
	tq.lock.Lock()
	defer tq.lock.Unlock()
	if task, ok := tq.Tasks[id]; !ok {
		return Task{}, errors.New("task not present, taskId : " + id) // TODO : use string concatination
	} else {
		delete(tq.Tasks, id)
		return task, nil
	}
}

// Update the task status in the map
func (tq *TaskQueue) UpdateStatus(id string, status string) error {
	tq.lock.Lock()
	defer tq.lock.Unlock()
	if task, ok := tq.Tasks[id]; !ok {
		return errors.New("task not present, taskId : " + id) // TODO : use string concatination
	} else {
		task.Status = status
		tq.Tasks[id] = task
		return nil
	}
}

// Check the status of all the task in the map
// and process it accordingly
func (tq *TaskQueue) CleanUp() {
	var err error
	for _, task := range tq.Tasks {
		switch task.Status {
		case Processing:
			// Task in-progress
			// Can remove the task which is in process for more than x secs/minutes
		case Completed:
			// remove from the queue
			err = tq.ProcessCompletedTask(task.Id)
		case Failed:
			// remove from the queue and retry if not timeout
			err = tq.ProcessFailedTask(task.Id)
		case Timeout:
			// remove from the queue and log
			err = tq.ProcessTimedOutTask(task.Id)
		}
		if err != nil {
			logrus.Errorln("Error : ", err)
		}
	}

}

// Remove the task from map
func (tq *TaskQueue) ProcessCompletedTask(id string) error {
	_, err := tq.RemoveTask(id)
	if err != nil {
		return fmt.Errorf("processCompletedTask: %w", err)
	}
	return nil
}

// Remove the task from map
// if its in queue for more than x time remove it and log
// else retry the task
func (tq *TaskQueue) ProcessFailedTask(id string) error {
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
		task.Status = Retry
		err := tq.AddNewTask(task)
		if err != nil {
			return fmt.Errorf("processFailedTask: %w", err)
		}
		return nil
	}
}

// Remove the task from map and log
func (tq *TaskQueue) ProcessTimedOutTask(id string) error {
	_, err := tq.RemoveTask(id)
	if err != nil {
		return fmt.Errorf("processTimedOutTask: %w", err)
	}
	logrus.Warnln("task timed out, taskId : ", id)
	return nil
}

// Add a new task to the queue channel
func producer(pings chan<- string, taskId string) {
	pings <- taskId
}

// Pull data from the queue channel
// and process the task
func worker(taskChannel <-chan string) {
	logrus.Infoln("Worker started")
	for taskId := range taskChannel {
		err := taskQueue.UpdateStatus(taskId, Processing)
		if err != nil {
			logrus.Errorln("worker error updating the status to processing, Error ", err)
		}
		err = ProcessTask(taskId)
		if err != nil {
			logrus.Errorln("worker error processing the Task, Error : ", err)
		}
	}
}

// Process the task
// For testing purpose: fail the task with probability of 1/3
func ProcessTask(taskId string) error {
	logrus.Infoln("Processing task : ", taskId)
	// failing tasks with probability of 1/3
	randomInt := rand.Intn(3)
	if randomInt == 2 {
		logrus.Infoln("failed : ", taskId)
		err := taskQueue.UpdateStatus(taskId, Failed)
		if err != nil {
			return errors.New("process task error updating the status to failed, Error : " + err.Error())
		}
		return nil
	}

	err := taskQueue.UpdateStatus(taskId, Completed)
	if err != nil {
		return errors.New("process task error updating the status to completed, Error : " + err.Error())
	}
	logrus.Infoln("Completed task : ", taskId)
	return nil
}

func main() {
	taskQueue = TaskQueue{}
	taskQueue.Init()

	// start clean up go-routine
	go func() {
		for {
			taskQueue.CleanUp()
			time.Sleep(1 * time.Second)
		}
	}()

	// start worker go-routine
	go func() {
		worker(taskQueue.Queue)
	}()

	// start producer
	// can run this in go routine to push new task in the queue (just need to call AddNewTask function)
	for i := 0; i < 10; i++ {
		task := Task{
			Id:           strconv.Itoa(i),
			Status:       Untouched,
			CreationTime: time.Now(),
			TaskData:     string(i),
		}
		err := taskQueue.AddNewTask(task)
		if err != nil {
			logrus.Errorln("Error adding new task, Err : ", err)
		}

		randomInt := rand.Intn(5)
		sleepTime := time.Duration(int64(randomInt)) * time.Second
		time.Sleep(sleepTime)
	}

	select {}

}
