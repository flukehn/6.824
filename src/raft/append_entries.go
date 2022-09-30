package raft
import "time"
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) heartbeats() {
	for rf.killed() == false {
		time.Sleep(150 * time.Millisecond)
		if rf.State() == LEADER {
			rf.SendHeartbeats()
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