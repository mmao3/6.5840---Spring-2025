package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "time"
import "sort"
import "encoding/json"
import "path/filepath"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doReduce(reply *RequestTaskReply, reducef func(string, []string) string) {
	pattern := reply.Task.InputFiles[0]
	matches, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatalf("failed to match files: %v", err)
	}
	var kva []KeyValue
	for _, filename := range matches {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v: %v", filename, err)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%d", reply.Task.TaskId)
	ofile, _ := ioutil.TempFile("", oname)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()
	os.Rename(ofile.Name(), oname)

	call("Coordinator.NotifyTaskComplete", &reply, new(struct{}))
}

func doMap(reply *RequestTaskReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(reply.Task.InputFiles[0])
	if err != nil {
		log.Fatalf("cannot open %v", reply.Task.InputFiles[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Task.InputFiles[0])
	}
	file.Close()
	kva := mapf(reply.Task.InputFiles[0], string(content))
	intermediate := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % reply.NReduce
		intermediate[r] = append(intermediate[r], kv)
	}
	for r, kva := range intermediate {
		oname := fmt.Sprintf("mr-%d-%d", reply.Task.TaskId, r)
		ofile, _ := ioutil.TempFile("", oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			enc.Encode(&kv)
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}

	call("Coordinator.NotifyTaskComplete", &reply, new(struct{}))
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		reply := RequestTaskReply{}
		ok := call("Coordinator.GetTask", new(struct{}), &reply)
		if !ok {
			break
		}
		switch reply.Task.TaskType {
		case Map:
			doMap(&reply, mapf)
		case Reduce:
			doReduce(&reply, reducef)
		case Waiting:
			time.Sleep(1 * time.Second)
		case Exit:
			os.Exit(0)
		default:
			time.Sleep(1 * time.Second)
		}
	}
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
