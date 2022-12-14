package raft
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
	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		//log.Printf("[%d] Term become %d\n", rf.me, args.Term)
		rf.Become(FOLLOWER)
		rf.votedFor = -1
		
	}
	LastLogIndex := rf.SnapshotIndex + len(rf.log)
	LastLogTerm := rf.SnapshotTerm
	if len(rf.log) > 0 {
		LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	if args.Term >= rf.currentTerm &&
		rf.votedFor == -1 &&
		(args.LastLogTerm > LastLogTerm || 
			(args.LastLogTerm == LastLogTerm && args.LastLogIndex >= LastLogIndex)){
		//DPrintf("[%d] get vote from [%d], with term %d LastLogTerm=%d, LastLogIndex=%d, and have term=%d loglen=%d, lastlogterm=%d\n", args.CandidateId, rf.me, args.Term, args.LastLogTerm, args.LastLogTerm, rf.currentTerm, len(rf.log)-1, rf.log[len(rf.log)-1].Term)
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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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