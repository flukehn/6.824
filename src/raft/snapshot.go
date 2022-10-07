package raft
import "time"
//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Snapshot []byte
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer rf.persist()
	reply.Term = rf.currentTerm
	if rf.currentTerm <= args.Term {
		if rf.State() != FOLLOWER {
			//DPrintf("[%d] become follower at Term=%d because appendentries\n", rf.me, args.Term)
		}
		rf.Become(FOLLOWER)
		rf.lastHeartbeat = time.Now()
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			//log.Printf("[%d] Term become %d\n", rf.me, args.Term)
			rf.votedFor = -1
		}
		//DPrintf("[%d] install snapshot\n", rf.me)
		rf.log = make([]LogEntry,0)
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
		rf.SnapshotIndex = args.LastIncludedIndex
		rf.SnapshotTerm = args.LastIncludedTerm
		rf.snapshot = args.Snapshot
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot: args.Snapshot,
			SnapshotTerm: args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		data := rf.persistdata()
		rf.persister.SaveStateAndSnapshot(data, args.Snapshot)
		//DPrintf("[%d] install snapshot success\n", rf.me)
		reply.Success = true
	} else {
		reply.Success = false
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *AppendEntriesReply, Ok chan bool) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	Ok <- ok
	//return ok
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//DPrintf("[%d] snapshot index=%d\n", rf.me, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("[%d] snapshoting index=%d\n", rf.me, index)
	//defer DPrintf("[%d] snapshot index=%d end\n", rf.me, index)
	if index <= rf.SnapshotIndex {
		return
	}
	st := index - rf.SnapshotIndex
	rf.SnapshotTerm  = rf.log[st-1].Term
	rf.snapshot = snapshot
	rf.SnapshotIndex = index
	rf.log = rf.log[st:]

	data := rf.persistdata()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	//SnapshotTerm := rf.SnapshotTerm
	//SnapshotIndex := rf.SnapshotIndex
	//rf.mu.Unlock()
	/*rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot: snapshot,
		SnapshotTerm: SnapshotTerm,//rf.SnapshotTerm,
		SnapshotIndex: SnapshotIndex,//rf.SnapshotIndex,
	}*/
}