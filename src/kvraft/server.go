package kvraft

import (
	"6.824/labgob"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"bytes"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister
	db map[string]string
	lastseq map[int64]int32
	waitch map[int]chan ExecResult
	lastexeindex int
}

func (kv* KVServer) InstallSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var lastseq map[int64]int32
	if d.Decode(&db) == nil {kv.db = db}
	if d.Decode(&lastseq) == nil {kv.lastseq = lastseq}
}

func (kv* KVServer) Snapshot() {
	for {
		time.Sleep(15*time.Millisecond)
		kv.mu.Lock()
		if kv.maxraftstate == -1 || kv.maxraftstate * 5 > kv.persister.RaftStateSize() {
			kv.mu.Unlock()
			continue
		}
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.db)
		e.Encode(kv.lastseq)
		kv.rf.Snapshot(kv.lastexeindex, w.Bytes())
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}