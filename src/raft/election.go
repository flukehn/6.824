package raft

import (
	"time"
	"math/rand"
)

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func (rf *Raft) StartVote(voteResult chan int) {
	rf.mu.Lock()
	rf.Become(CANDIDATE)
	DPrintf("[%d] waiting for vote with term %d\n", rf.me, rf.currentTerm+1)
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
				LastLogIndex := rf.SnapshotIndex + len(rf.log)
				LastLogTerm := rf.SnapshotTerm
				if len(rf.log) > 0{
					LastLogTerm = rf.log[len(rf.log)-1].Term
				}
				args := RequestVoteArgs{
					Term: rf.currentTerm,
					CandidateId: rf.me,
					LastLogIndex: LastLogIndex,
					LastLogTerm: LastLogTerm,
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
						ret = 0
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
	
	// need optimize !!!
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
			//rf.Become(LEADER)
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
		if rf.State() == FOLLOWER && time.Since(rf.lastHeartbeat) > 600 * time.Millisecond {
			//start vote
			rf.mu.Unlock()
			voteResult := make(chan int)
			go rf.StartVote(voteResult)
			select {
			case result := <- voteResult:
				if result  == 0 {
					rf.Become(FOLLOWER)
				} else {
					rf.mu.Lock()
					DPrintf("[%d] become leader with term %d\n", rf.me, rf.currentTerm)
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					rf.appendRunning = make([]bool, len(rf.peers))
					//rf.notconn = make(map[int]bool)
					rf.appendTime = make([]time.Time, len(rf.peers))
					//rf.commitIndex = rf.lastApplied
					for i := range rf.nextIndex {
						rf.nextIndex[i] = len(rf.log) + rf.SnapshotIndex + 1
						rf.matchIndex[i] = 0
						rf.appendRunning[i] = false
						rf.appendTime[i] = time.Now()
					}
					rf.Become(LEADER)
					rf.mu.Unlock()
					//rf.SendHeartbeats()
					//go func() {
					rf.cmdnotify <- -1
					//}()
				}
				
			case <-time.After(150*time.Millisecond):
				//log.Printf("candidate [%d] vote time out\n", rf.me)
				rf.Become(FOLLOWER)
			}
		} else {
			rf.mu.Unlock()
		}
	}
}