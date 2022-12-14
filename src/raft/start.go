package raft
import (
	"time"
)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.State() != LEADER {
		return -1, -1, false
	}

	term = rf.currentTerm
	index = len(rf.log) + rf.SnapshotIndex + 1
	rf.log = append(rf.log, LogEntry{
		Term: term,
		Msg: ApplyMsg{
			CommandValid: true,
			Command: command,
			CommandIndex: index,
		},
	})
	//DPrintf("[%d] get a cmd=%v index %d at term %d\n", rf.me, command, index, rf.currentTerm)
	go func(index int) {
		rf.cmdnotify <- index
	}(index)

	return index, term, isLeader
}

func (rf *Raft) Exec() {
	for {
		time.Sleep(10*time.Millisecond)
		rf.mu.Lock()
		/*if len(rf.log) >= 2 {
			//DPrintf("[%d] commit = %d, apply = %d\n", rf.me, rf.commitIndex, rf.lastApplied)
		}*/
		/*if rf.commitIndex >= len(rf.log) {
			//DFatalf("[%d] rf.commitIndex=%d >= len(rf.log)=%d\n", rf.me, rf.commitIndex, len(rf.log))
		}*/
		for ;rf.lastApplied < rf.commitIndex && rf.lastApplied < len(rf.log) + rf.SnapshotIndex; rf.lastApplied++ {
			Msg := rf.log[rf.lastApplied-rf.SnapshotIndex].Msg
			rf.mu.Unlock()
			rf.applyCh <- Msg
			//DPrintf("[%d] exec index %d cmd=%v\n", rf.me, rf.lastApplied+1,Msg.Command)
			rf.mu.Lock()
			//DPrintf("[%d] exec end\n", rf.me)	
		}
		rf.mu.Unlock()
	}
}