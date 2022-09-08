package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io"
import "encoding/json"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		var reply RequestReply
		for {
			reply = RequestReply{}
			ok := CallRequestTask(&reply)
			//fmt.Printf("Task: %s\n", reply.Task)
			for !ok || reply.Task == "none"{
				return
			}
			if reply.Task != "wait" {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if reply.Task == "map" {
			filename := reply.Filename
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			//var kvb [R][]KeyValue
			kvb := make([][]KeyValue, reply.R)
			for _, kv := range kva{
				h := ihash(kv.Key) % reply.R
				kvb[h] = append(kvb[h], kv)
			}
			for i, kvv := range kvb{
				tfn := fmt.Sprintf("/tmp/map-%d-%d.out",reply.Id,i)
				tf, err := os.Create(tfn)
				if err != nil{
					log.Fatalf("cannot write %v", tfn)
				}
				enc := json.NewEncoder(tf)
				for _, kv := range kvv{
					enc.Encode(&kv)
					//fmt.Fprintf(tf, "%v %v\n", key, value)
				}
				tf.Close()
			}
			//intermediate = append(intermediate, kva...)
		} else {
			intermediate := []KeyValue{}
			for i := 0; i < reply.M; i++{
				tfn := fmt.Sprintf("/tmp/map-%d-%d.out",i,reply.Id)
				tf, err := os.Open(tfn)
				if err != nil {
					log.Fatalf("cannot read %v", tfn)
				}
				dec := json.NewDecoder(tf)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				tf.Close()
			}
			sort.Sort(ByKey(intermediate))
			oname := fmt.Sprintf("mr-out-%d", reply.Id)
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			ofile.Close()
		}
		CallTaskDone(reply.Id, reply.Task)
	}
}
func CallTaskDone(id int, task string) {
	args := TaskDoneArgs{id, task}
	reply := TaskDoneReply{}
	call("Coordinator.TaskDone", &args, &reply)
}
func CallRequestTask(reply *RequestReply) bool {
	args := RequestArgs{}
	//reply := RequestReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		fmt.Printf("CallRequestTask failed!\n")
		return false
	}
	return true
}
//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
