package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"time"
)

const (
	FOLLOWER int32 = iota
	LEADER
	CANDIDATE
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//

type LogEntry struct {
	Term int
	Msg ApplyMsg
}

type Conn struct {
	Id int
	Ok bool
	//T time.Time
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state int32
	lastHeartbeat time.Time
	currentLeader int
	currentTerm int
	votedFor int
	// log
	commitIndex int
	lastApplied int
	nextIndex []int
	matchIndex []int
	appendRunning []bool
	log []LogEntry
	cmdnotify chan int
	conn chan Conn
	notconn map[int]bool
	//apply
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.State() == LEADER
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//DPrintf("[%d] persist()\n", rf.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//e.Encode(rf.state)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	//e.Encode(rf.commitIndex)
	//e.Encode(rf.lastApplied)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	DPrintf("[%d] readPersist\n", rf.me)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	//time.Sleep(100*time.Millisecond)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var votedFor, currentTerm int
	//var commitIndex, lastApplied int
	var log []LogEntry
	if d.Decode(&currentTerm) == nil {rf.currentTerm = currentTerm}
	if d.Decode(&votedFor) == nil {rf.votedFor = votedFor}
	//if d.Decode(&commitIndex) == nil {rf.commitIndex = commitIndex}
	//if d.Decode(&lastApplied) == nil {rf.lastApplied = lastApplied}
	if d.Decode(&log) == nil {
		rf.log = log
		//DPrintf("[%d] Persist log len=%d\n", rf.me, len(rf.log))
	} else {
		DPrintf("[%d] could not decode log\n", rf.me);
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) State() int32 {
	z := atomic.LoadInt32(&rf.state)
	return z
}

func (rf *Raft) Become(z int32) {
	atomic.StoreInt32(&rf.state, z)
	//w := []string{"Follower", "Candidate", "LEADER"}
	//DPrintf("[%d] Become %s\n", rf.me, w[z])
}
