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
	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
	"time"
	"math/rand"
	"log"
	//"io/ioutil"
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
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		//log.Printf("[%d] Term become %d\n", rf.me, args.Term)
		rf.Become(FOLLOWER)
		rf.votedFor = -1
		
	}
	if args.Term >= rf.currentTerm && rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		*reply = RequestVoteReply{
			rf.currentTerm,
			true,
		}
	} else {
		*reply = RequestVoteReply{
			rf.currentTerm,
			false,
		}
	}
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int //so follower can redirect clients
	PrevLogIndex int //index of log entry immediately preceding new ones
	PrevLogTerm int //term of prevLogIndex entry
	Entries[] ApplyMsg //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int //leader’s commitIndex
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

/*
1. Reply false if term < currentTerm (§5.1)
2. Reply false if log doesn’t contain an entry at prevLogIndex
whose term matches prevLogTerm (§5.3)
3. If an existing entry conflicts with a new one (same index
but different terms), delete the existing entry and all that
follow it (§5.3)
4. Append any new entries not already in the log
5. If leaderCommit > commitIndex, set commitIndex =
min(leaderCommit, index of last new entry)
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm <= args.Term {
		
		rf.Become(FOLLOWER)
		rf.lastHeartbeat = time.Now()
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			//log.Printf("[%d] Term become %d\n", rf.me, args.Term)
			rf.votedFor = -1
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	if rf.State() != LEADER {
		return -1, -1, false
	}

	return index, term, isLeader
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
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func (rf *Raft) StartVote(voteResult chan int) {
	rf.mu.Lock()
	rf.Become(CANDIDATE)
	//log.Printf("server [%d] waiting for vote with term %d\n", rf.me, rf.currentTerm+1)
	rf.currentTerm += 1
	
	rf.votedFor = rf.me
	//Term := rf.currentTerm
	rf.mu.Unlock()
	var count int = 1
	var tot int = len(rf.peers)
	vote_grant := make(chan int)
	
	for i := range rf.peers {
		if rf.State() != CANDIDATE {
			break
		}
		if i != rf.me {
			//tot += 1
			go func(id int){
				//log.Printf("[%d] request [%d] vote\n", rf.me, id)
				rf.mu.Lock()
				args := RequestVoteArgs{
					Term: rf.currentTerm,
					CandidateId: rf.me,
					LastLogIndex: rf.commitIndex,
					LastLogTerm: rf.lastApplied,
				}
				rf.mu.Unlock()
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(id, &args, &reply)
				var ret int
				if ok {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						//log.Printf("[%d] Term become %d\n", rf.me, reply.Term)
						rf.votedFor = -1
						rf.Become(FOLLOWER)
						rf.mu.Unlock()
						ret = -1//vote_grant <- -1
					} else if reply.VoteGranted {
						rf.mu.Unlock()
						ret = 1
						//vote_grant <- 1
					} else {
						rf.mu.Unlock()
					}
				} else {
					ret = 0
					//vote_grant <- 0
				}
				//log.Printf("[%d] get [%d] vote %d\n", rf.me, id, ret)
				vote_grant <- ret
				
			}(i)
		}
	}
	
	for i := 0; i < tot - 1; i++ {
		v := <- vote_grant
		//log.Printf("server %d get a result %d\n", rf.me, v)
		/*if rf.State() != CANDIDATE {
			continue
		}*/
		if v < 0 {
			break
		} else {
			count += v
		}
		if count * 2 > tot {
			rf.Become(LEADER)
			//log.Printf("candidate [%d] become leader", rf.me)
			voteResult <- 1
			return
		}
	}
	//log.Printf("candidate [%d] granted %d votes, but need %d\n", rf.me, count, (tot+1)/2)
	voteResult <- 0
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		timelimit := time.Duration(300 + rand.Intn(300)) * time.Millisecond
		//log.Printf("server %d timelimit = %v\n", rf.me, timelimit)
		time.Sleep(timelimit)
		//log.Printf("server %d 's ticker\n", rf.me)
		rf.mu.Lock()
		//log.Printf("server %d 's ticker_s\n", rf.me)
		if rf.State() == FOLLOWER && time.Since(rf.lastHeartbeat) > 200 * time.Millisecond {
			//start vote
			rf.mu.Unlock()
			voteResult := make(chan int)
			go rf.StartVote(voteResult)
			select {
			case result := <- voteResult:
				if result  == 0 {
					rf.Become(FOLLOWER)
				} else {
					rf.SendHeartbeats()
				}
				
			case <-time.After(timelimit):
				//log.Printf("candidate [%d] vote time out\n", rf.me)
				rf.Become(FOLLOWER)
			}
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) SendHeartbeats() {
	Term := make(chan int)
	for i := range rf.peers {
		if rf.State() != LEADER {
			break
		}
		if i != rf.me {
			//log.Printf("leader %d send heartbeat to %d\n", rf.me, i)
			go func(id int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{
					Term: rf.currentTerm,
				}
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(id, &args, &reply)
				if ok {
					Term <- reply.Term
				} else {
					Term <- -1
				}
			} (i)
		}
	}
	var count int = 0
	for i := 0; i < len(rf.peers) - 1; i++{
		//log.Printf("%d reading\n", rf.me);
		v := <- Term
		//log.Printf("%d end read\n", rf.me);
		rf.mu.Lock()
		if v > rf.currentTerm {
			rf.currentTerm = v
			rf.Become(FOLLOWER)
			rf.votedFor = -1
			rf.mu.Unlock()
			break
		} else if v >= 0 {
			count += 1
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
	}
	if count * 2 <= len(rf.peers) && rf.State() == LEADER {
		rf.Become(FOLLOWER)
	}
}

func (rf *Raft) heartbeats() {
	for rf.killed() == false {
		time.Sleep(150 * time.Millisecond)
		if rf.State() == LEADER {
			rf.SendHeartbeats()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	//log.Printf("%d %d %d\n", FOLLOWER, LEADER, CANDIDATE)
	log.SetFlags(log.Ltime | log.Lmicroseconds | log.Lshortfile)
	//log.SetOutput(ioutil.Discard)
	//log.SetFlags(0)
	rf.state = FOLLOWER
	rf.currentLeader = -1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastHeartbeat = time.Now()
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.heartbeats()

	return rf
}
