package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	wid := strconv.Itoa(os.Getpid())
	log.Printf("Worker %s start\n", wid)

	var lastTaskType string
	var lastTaskIndex int
	for {
		args := CallGetTaskArgs{
			WorkerID:      wid,
			LastTaskType:  lastTaskType,
			LastTaskIndex: lastTaskIndex,
		}
		reply := ReplyGetTaskArgs{}
		call("Coordinator.HandleGetTask", &args, &reply)
		if reply.TaskType == "" {
			// 作业完毕
			log.Printf("Received job finish signal from Coordinator\n")
			break
		}

		log.Printf("Received task %s-%d.\n", reply.TaskType, reply.TaskIndex)
		if reply.TaskType == "MAP" {
			file, err := os.Open(reply.InputFile)
			if err != nil {
				log.Fatalf("Fail to open MAP input flie %s: %e\n", reply.InputFile, err)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("Fail to read MAP input file %s: %e\n", reply.InputFile, err)
			}
			// MAP处理得到中间结果
			kva := mapf(reply.InputFile, string(content))
			// 分片
			hashedKva := make(map[int][]KeyValue)
			for _, kv := range kva {
				hashed := ihash(kv.Key) % reply.ReduceNum
				hashedKva[hashed] = append(hashedKva[hashed], kv)
			}
			// 中间结果写入临时文件
			for i := 0; i < reply.ReduceNum; i++ {
				oFile, err := os.Create(genTmpMapOutFile(wid, reply.TaskIndex, i))
				if err != nil {
					log.Fatalf("Fail to create MAP output temp flie: %e\n", err)
				}
				for _, kv := range hashedKva[i] {
					fmt.Fprintf(oFile, "%v\t%v\n", kv.Key, kv.Value)
				}
				oFile.Close()
			}
		} else if reply.TaskType == "REDUCE" {
			var lines []string
			for mi := 0; mi < reply.MapNum; mi++ {
				inputFile := genFinalMapOutFile(mi, reply.TaskIndex)
				file, err := os.Open(inputFile)
				if err != nil {
					log.Fatalf("Fail to open MAP output flie %s: %e\n", inputFile, err)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("Fail to read MAP output flie %s: %e\n", inputFile, err)
				}
				lines = append(lines, strings.Split(string(content), "\n")...)
			}
			var kva []KeyValue
			for _, line := range lines {
				if strings.TrimSpace(line) == "" {
					continue
				}
				parts := strings.Split(line, "\t")
				kva = append(kva, KeyValue{
					Key:   parts[0],
					Value: parts[1],
				})
			}
			// 根据key排序
			sort.Sort(ByKey(kva))

			oFile, err := os.Create(genTmpReduceOutFile(wid, reply.TaskIndex))
			if err != nil {
				log.Fatalf("Fail to create REDUCE output temp flie: %e\n", err)
			}
			i := 0 // 前哨兵
			for i < len(kva) {
				j := i + 1 // 后哨兵
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				rOutput := reducef(kva[i].Key, values)
				fmt.Fprintf(oFile, "%v %v\n", kva[i].Key, rOutput)
				i = j
			}
			oFile.Close()
		}
		lastTaskType = reply.TaskType
		lastTaskIndex = reply.TaskIndex
		log.Printf("Finished task %s-%d", reply.TaskType, reply.TaskIndex)
	}
	log.Printf("Worker %s exit\n", wid)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
