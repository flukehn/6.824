package raft

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
	DPrintf("[%d] snapshot index=%d\n", rf.me, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("[%d] snapshoting index=%d\n", rf.me, index)
	defer DPrintf("[%d] snapshot index=%d end\n", rf.me, index)
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