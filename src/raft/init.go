package raft
import (
	"6.824/labrpc"
	"time"
	"log"
	//"io/ioutil"
)
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
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0});
	rf.lastHeartbeat = time.Now()
	rf.applyCh = applyCh
	rf.cmdnotify = make(chan int)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	//go rf.heartbeats()

	go rf.DistroCmd()
	
	go rf.Exec()

	return rf
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