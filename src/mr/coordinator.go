package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "fmt"


type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	NReduce int

	TotalMapTasks int
	CompletedMapTasks int

	MapTasks []MapReduceTask
	ReduceTasks []MapReduceTask
	
	TotalReduceTasks int
	CompletedReduceTasks int
}

func assignTask(taskList []MapReduceTask, nReduce int, reply *RequestTaskReply) {
	for index, task := range taskList {
		if task.TaskStatus == Available || (task.TaskStatus == Pending && time.Since(task.StartTime) > 10*time.Second) {
			// Assign task to worker
			reply.Task = taskList[index]
			reply.NReduce = nReduce
			taskList[index].TaskStatus = Pending
			taskList[index].StartTime = time.Now()
			return
		}
	}

	// No task available now; tell worker to wait
	reply.Task.TaskType = Waiting
}

func (c *Coordinator) GetTask(args *Empty, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.CompletedReduceTasks == c.TotalReduceTasks {
		reply.Task.TaskType = Exit
	} else if c.CompletedMapTasks == c.TotalMapTasks {
		assignTask(c.ReduceTasks, c.NReduce, reply)
	} else {
		assignTask(c.MapTasks, c.NReduce, reply)
	}
	return nil
}

func markTaskComplete(taskList []MapReduceTask, taskId int) {
	for index, task := range taskList {
		if task.TaskId == taskId {
			taskList[index].TaskStatus = Completed
			taskList[index].EndTime = time.Now()
			break
		}
	}
}

// Notify the coordinator that a task has been completed
func (c *Coordinator) NotifyTaskComplete(args *RequestTaskReply, reply *Empty) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Task.TaskType == Map {
		markTaskComplete(c.MapTasks, args.Task.TaskId)
		c.CompletedMapTasks++
	} else {
		markTaskComplete(c.ReduceTasks, args.Task.TaskId)
		c.CompletedReduceTasks++
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.CompletedMapTasks == c.TotalMapTasks &&
       c.CompletedReduceTasks == c.TotalReduceTasks
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce:             nReduce,
		TotalMapTasks:       len(files),
		CompletedMapTasks:   0,
		MapTasks:            make([]MapReduceTask, len(files)),
		ReduceTasks:         make([]MapReduceTask, nReduce),
		TotalReduceTasks:    nReduce,
		CompletedReduceTasks: 0,
	}

	// Initialize mapTasks
	for i := range c.MapTasks {
		c.MapTasks[i] = MapReduceTask{
			TaskType:    Map,
			TaskStatus:  Available,
			TaskId:      i,
			InputFiles:  []string{files[i]},
			OutputFiles: nil,
		}
	}

	// Initialize reduceTasks
	for i := range c.ReduceTasks {
		c.ReduceTasks[i] = MapReduceTask{
			TaskType:    Reduce,
			TaskStatus:  Available,
			TaskId:      i,
			InputFiles:  []string{fmt.Sprintf("mr-*-%d", i)}, // this is a pattern you may expand during reduce
			OutputFiles: []string{fmt.Sprintf("mr-out-%d", i)},
		}
	}

	c.server()
	return &c
}
