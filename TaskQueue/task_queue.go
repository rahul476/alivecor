package TaskQueue

import (
	"github.com/pkg/errors"
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

// Initialise the task map and queue
func (tq *TaskQueue) Init(queueSize int) {
	tq.Tasks = make(map[string]Task, 0)
	tq.Queue = make(chan string, queueSize)
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

// Add a new task to the queue channel
func producer(pings chan<- string, taskId string) {
	pings <- taskId
}
