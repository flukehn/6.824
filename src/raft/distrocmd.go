package raft

import (
	"time"
	"sort"
	"log"
)
var _ = log.Printf
func (rf *Raft) DistroCmd() {
	for {
		select{
		case <-rf.cmdnotify:
			//
		case <-time.After(150*time.Millisecond):
			//
		}
		go func() {
			if rf.State() != LEADER{
				return
			}
			rf.mu.Lock()
			this_term := rf.currentTerm
			//DPrintf("[%d] distrocmd\n", rf.me)
			rf.mu.Unlock()
			Term := make(chan int)
			for i := range rf.peers {
				if rf.State() != LEADER {
					break
				}
				if i != rf.me {
					//log.Printf("leader %d send heartbeat to %d\n", rf.me, i)
					go func(id int) {
						var del_len = 1
						for rf.State()==LEADER{
							rf.mu.Lock()
							var nxt int = len(rf.log)
							args := AppendEntriesArgs{
								Term: rf.currentTerm,
								LeaderId: rf.me,
								LeaderCommit: rf.commitIndex,
								PrevLogIndex: rf.nextIndex[id]-1,
								PrevLogTerm: rf.log[rf.nextIndex[id]-1].Term,
								Entries: rf.log[rf.nextIndex[id]:],
							}
							rf.mu.Unlock()
							reply := AppendEntriesReply{}
							ok := rf.sendAppendEntries(id, &args, &reply)
							if ok {
								rf.mu.Lock()
								if reply.Success{
									rf.matchIndex[id] = nxt-1
									rf.nextIndex[id] = nxt
									rf.mu.Unlock()
									//log.Printf("[%d] matched %d\n", id, nxt-1)
									Term <- 1
									return
								} else if reply.Term > rf.currentTerm{
									//DPrintf("[%d] become follwer with because a server has large term=%d\n", rf.me, reply.Term)
									rf.Become(FOLLOWER)
									rf.currentTerm = reply.Term
									rf.votedFor=-1
									rf.mu.Unlock()
									return
								} else {
									rf.nextIndex[id] = max(rf.nextIndex[id] - del_len, rf.matchIndex[id]+1)
									rf.mu.Unlock()
									del_len *= 2
								}
							} else {
								Term <- -1
								return
							}
						}
						Term <- -1
					} (i)
				}
			}
			var count int = 0
			for i := 0; i < len(rf.peers) - 1  && rf.State() == LEADER; i++{
				v := <- Term
				if v < 0 {
					count += 1
				}
				rf.mu.Lock()
				if rf.currentTerm != this_term {
					rf.mu.Unlock()
					break
				} else {
					rf.mu.Unlock()
				}
				if count * 2 >= len(rf.peers) {
					//DPrintf("[%d] become follower with distrocmd failed\n", rf.me)
					rf.Become(FOLLOWER)
					break
				}
			}
		} ()
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.State() != LEADER {
				return
			}
			
			rf.matchIndex[rf.me] = len(rf.log)-1
			
			match := make([]int, len(rf.peers))
			copy(match, rf.matchIndex)
			sort.Slice(match, func(i, j int) bool {
				return match[i] < match[j]
			})
			/*for i, m := range rf.matchIndex {
				log.Printf("[%d] match %d\n",i, m)
			} */
			p := match[(len(rf.peers)-1)/2]
			//log.Printf("[%d] commit %d, tot_len = %d\n", rf.me, p, len(rf.log))
			if rf.log[p].Term == rf.currentTerm{
				rf.commitIndex = max(rf.commitIndex, p)
			}
		}()
		//DPrintf("[%d] distrocmd\n", rf.me)
	}
}