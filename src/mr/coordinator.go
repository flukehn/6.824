package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	M, R int
	files []string
	mapstatus []int
	reducestatus []int
	mapexec []time.Time
	reduceexec []time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Task == "map" {
		c.mapstatus[args.Id] = 2	
	} else {
		c.reducestatus[args.Id] = 2
	}
	return nil
}
func (c *Coordinator) RequestTask(args *RequestArgs, reply *RequestReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	allocated := false
	mapachive := true
	reduceachive := true
	for i, v := range c.mapstatus{
		if v == 2 {
			continue
		}
		mapachive = false
		if v == 1 && time.Since(c.mapexec[i]) > 10 * time.Second {
			c.mapstatus[i] = 0
			v = 0
		}
		if v == 0{
			c.mapstatus[i] = 1
			*reply = RequestReply{
				Id: i,
				Task: "map",
				R: c.R,
				Filename: c.files[i],
			}
			c.mapexec[i] = time.Now()
			allocated = true
			break
		}
		
	}
	if !allocated && mapachive{
		for i, v := range c.reducestatus {
			if v == 2 {
				continue
			}
			reduceachive = false
			if v == 1 && time.Since(c.reduceexec[i]) > 10 * time.Second {
				c.reducestatus[i] = 0
				v = 0
			}
			if v == 0 {
				c.reducestatus[i] = 1
				*reply = RequestReply{
					Id: i,
					Task: "reduce",
					M: c.M,
				}
				c.reduceexec[i] = time.Now()
				allocated = true
				break
			}
		}
	}
	if !allocated {
		if mapachive && reduceachive{
			*reply = RequestReply{Task: "none"}
		} else {
			*reply = RequestReply{Task: "wait"}
		}
	}
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	//ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, v := range c.reducestatus{
		if v != 2 {
			return false
		}
	}

	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files: files,
		M: len(files),
		R: nReduce,
	}
	// Your code here.
	c.mapstatus = make([]int, c.M)
	c.reducestatus = make([]int, c.R)
	c.mapexec = make([]time.Time, c.M)
	c.reduceexec = make([]time.Time, c.R)
	c.server()
	return &c
}
