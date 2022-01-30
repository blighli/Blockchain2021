package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	lock           sync.Mutex
	stage          string
	nMap           int
	nReduce        int
	tasks          map[string]Task
	availableTasks chan Task
}

type Task struct {
	Type         string // MAP or REDUCE
	Index        int
	MapInputFile string
	WorkerID     string
	Deadline     time.Time
}

// 初始化Coordinator
func MakeCoordinator(files []string, numOfReduce int) *Coordinator {
	c := Coordinator{
		stage:          "MAP",
		nMap:           len(files),
		nReduce:        numOfReduce,
		tasks:          make(map[string]Task),
		availableTasks: make(chan Task, int(math.Max(float64(len(files)), float64(numOfReduce)))),
	}
	for i, file := range files {
		task := Task{
			Type:         "MAP",
			Index:        i,
			MapInputFile: file,
		}
		c.tasks[GenTaskID(task.Type, task.Index)] = task
		c.availableTasks <- task
	}
	log.Printf("Coordinator started\n")
	c.server()

	// Task轮询回收
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.lock.Lock()
			for _, task := range c.tasks {
				if task.WorkerID != "" && time.Now().After(task.Deadline) {
					log.Printf("Time-outed task %s-%d found on worker %s, prepare to re-allocate", task.Type, task.Index, task.WorkerID)
					task.WorkerID = ""
					c.availableTasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()

	return &c
}

// 处理获取task的请求
func (c *Coordinator) HandleGetTask(args *CallGetTaskArgs, reply *ReplyGetTaskArgs) error {
	// 处理worker的上一个task
	if args.LastTaskType != "" {
		c.lock.Lock()
		lastTaskId := GenTaskID(args.LastTaskType, args.LastTaskIndex)
		// 与coordinator记录的处理中任务匹配
		if task, exist := c.tasks[lastTaskId]; exist && task.WorkerID == args.WorkerID {
			// 向日志输出该任务已完成
			log.Printf("Task %s-%d is finished on worker %s", task.Type, task.Index, task.WorkerID)
			// 将临时文件标记为最终文件（重命名）
			if task.Type == "MAP" {
				for ri := 0; ri < c.nReduce; ri++ {
					err := os.Rename(
						genTmpMapOutFile(args.WorkerID, args.LastTaskIndex, ri),
						genFinalMapOutFile(args.LastTaskIndex, ri),
					)
					if err != nil {
						log.Fatalf("Fail to mark map output file `%s` as final.\n", genTmpMapOutFile(args.WorkerID, args.LastTaskIndex, ri))
					}
				}
			} else if task.Type == "REDUCE" {
				err := os.Rename(
					genTmpReduceOutFile(args.WorkerID, args.LastTaskIndex),
					genFinalReduceOutFile(args.LastTaskIndex),
				)
				if err != nil {
					log.Fatalf("Fail to mark reduce output file `%s` as final.\n", genTmpReduceOutFile(args.WorkerID, args.LastTaskIndex))
				}
			}
			delete(c.tasks, lastTaskId)

			// 所有Task已完成，进入下一阶段
			if len(c.tasks) == 0 {
				c.transit()
			}
		}
		c.lock.Unlock()
	}

	// 为worker分配新的task
	task, exist := <-c.availableTasks
	if !exist {
		return nil
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	log.Printf("%s task %d allocated for worker %s\n", task.Type, task.Index, args.WorkerID)
	task.WorkerID = args.WorkerID
	task.Deadline = time.Now().Add(10 * time.Second)
	c.tasks[GenTaskID(task.Type, task.Index)] = task
	reply.TaskType = task.Type
	reply.TaskIndex = task.Index
	reply.InputFile = task.MapInputFile
	reply.MapNum = c.nMap
	reply.ReduceNum = c.nReduce
	return nil
}

func (c *Coordinator) transit() {
	if c.stage == "MAP" {
		log.Printf("All MAP tasks are finished, transit to REDUCE stage.\n")
		c.stage = "REDUCE"
		for i := 0; i < c.nReduce; i++ {
			task := Task{
				Type:  "REDUCE",
				Index: i,
			}
			c.tasks[GenTaskID(task.Type, task.Index)] = task
			c.availableTasks <- task
		}
	} else if c.stage == "REDUCE" {
		log.Printf("All REDUCE tasks are finished, work done.\n")
		close(c.availableTasks)
		c.stage = ""
	}
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
	ret := false

	c.lock.Lock()
	defer c.lock.Unlock()
	ret = (c.stage == "")

	return ret
}

// 生成taskID，由类型和index构成
func GenTaskID(tt string, index int) string {
	return fmt.Sprintf("%s-%d", tt, index)
}

// 生成各阶段（temp，final）各类型（map，reduce）的产出文件名
func genTmpMapOutFile(worker string, mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("tmp-worker-%s-%d-%d", worker, mapIndex, reduceIndex)
}

func genFinalMapOutFile(mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
}

func genTmpReduceOutFile(worker string, reduceIndex int) string {
	return fmt.Sprintf("tmp-worker-%s-out-%d", worker, reduceIndex)
}

func genFinalReduceOutFile(reduceIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceIndex)
}
