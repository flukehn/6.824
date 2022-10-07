package raft

import (
	"time"
	"sort"
	//"log"
)
//var _ = log.Printf

func (rf *Raft) CheckMajority() {
	for {
		time.Sleep(150*time.Millisecond)
		rf.mu.Lock()
		if rf.State() != LEADER {
			rf.mu.Unlock()
			continue
		}
		var count int = 0
		for i := range rf.peers {
			if i != rf.me && time.Since(rf.appendTime[i]) > 1200*time.Millisecond {
				count += 1
			}
			if count * 2 >= len(rf.peers) {
				rf.Become(FOLLOWER)
				DPrintf("[%d] become follower with distrocmd failed\n", rf.me)
			}
		}
		rf.mu.Unlock()
	}
	/*for v := range rf.conn{
		if !v.Ok{DPrintf("[%d] conn %v\n", rf.me, v)}
		if rf.State() != LEADER {continue}
		rf.mu.Lock()
		
		if v.Ok {
			_, ok := rf.notconn[v.Id]
			if ok {
				delete(rf.notconn, v.Id)
			}
		} else {
			rf.notconn[v.Id] = true
			if len(rf.notconn) * 2 >= len(rf.peers) {
				rf.Become(FOLLOWER)
				DPrintf("[%d] become follower with distrocmd failed\n", rf.me)
			}	
		}
		rf.mu.Unlock()
	}*/
}

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
			//rf.mu.Lock()
			//this_term := rf.currentTerm
			//DPrintf("[%d] distrocmd\n", rf.me)
			//rf.mu.Unlock()
			//Term := make(chan int)
			for i := range rf.peers {
				if rf.State() != LEADER {
					break
				}
				if i != rf.me {
					//DPrintf("leader [%d] send heartbeat to [%d]\n", rf.me, i)
					go func(id int) {
						
						rf.mu.Lock()
						if rf.appendRunning[id] {
							rf.mu.Unlock()
							//Term <- 1
							return
						}
						rf.appendRunning[id]=true
						rf.mu.Unlock()
						
						var del_len = 1
						for {
							rf.mu.Lock()
							if rf.State() != LEADER {
								rf.appendRunning[id]=false
								rf.mu.Unlock()
								return
							}
							
							var ok bool
							Ok := make(chan bool)
							st := rf.nextIndex[id]-rf.SnapshotIndex-1
							reply := AppendEntriesReply{}
							var nxt int
							if st >= 0 {
								var Tran_Entries []LogEntry
								if del_len == 1 {
									Tran_Entries = rf.log[st:]
								} else {
									length := min(len(rf.log), st+del_len)
									Tran_Entries = rf.log[st:length]
								}
								
								//Tran_Entries = rf.log[rf.nextIndex[id]:]
								nxt = rf.nextIndex[id]+len(Tran_Entries)
								PrevLogTerm := rf.SnapshotTerm
								if st >= 1 {
									PrevLogTerm = rf.log[st-1].Term
								}
								args := AppendEntriesArgs{
									Term: rf.currentTerm,
									LeaderId: rf.me,
									LeaderCommit: rf.commitIndex,
									PrevLogIndex: rf.nextIndex[id]-1,
									PrevLogTerm: PrevLogTerm,
									Entries: Tran_Entries,
									//Entries: rf.log[rf.nextIndex[id]:],
								}
								rf.mu.Unlock()
								//DPrintf("leader [%d] send heartbeat to [%d]\n", rf.me, id)
								go rf.sendAppendEntries(id, &args, &reply, Ok)
							} else {
								DFatalf("need snapshot GG\n")
							}
							select {
							case <- time.After(500*time.Millisecond):
								continue
							case ok= <- Ok:
							
								if ok {
									rf.mu.Lock()
									rf.appendTime[id] = time.Now()
									if reply.Success{
										rf.matchIndex[id] = nxt-1
										rf.nextIndex[id] = nxt
										rf.appendRunning[id]=false
										rf.mu.Unlock()
										//rf.conn <- Conn{id, true}
										//log.Printf("[%d] matched %d\n", id, nxt-1)
										
										return
									} else if reply.Term > rf.currentTerm{
										DPrintf("[%d] become follower with because a server has large term=%d\n", rf.me, reply.Term)
										rf.Become(FOLLOWER)
										rf.currentTerm = reply.Term
										rf.votedFor=-1
										rf.appendRunning[id]=false
										rf.mu.Unlock()
										//Term <- -1
										return
									} else {
										rf.nextIndex[id] = max(rf.nextIndex[id] - del_len, rf.matchIndex[id]+1)
										del_len = min(del_len * 2, rf.SnapshotIndex + len(rf.log) + 1 - rf.nextIndex[id])
										rf.mu.Unlock()
										//rf.conn <- Conn{id, true}
									}
								} else {
									//DPrintf("[%d] send to [%d] timeout\n", rf.me, id)
									//rf.mu.Lock()
									//rf.appendRunning[id]=false
									//rf.mu.Unlock()
									//rf.conn <- Conn{id, false}
									//return
								}
							}
						}
					} (i)
				}
			}
			/*
			var count int = 0
			for i := 0; i < len(rf.peers) - 1  && rf.State() == LEADER; i++{
				v := <- Term
				if v < 0 {
					count += 1
				}
				rf.mu.Lock()
				if rf.currentTerm != this_term  || rf.State() != LEADER{
					rf.mu.Unlock()
					break
				} else {
					rf.mu.Unlock()
				}
				if count * 2 >= len(rf.peers) {
					
					rf.Become(FOLLOWER)
					break
				}
			}
			*/
		} ()
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.State() != LEADER {
				return
			}
			
			rf.matchIndex[rf.me] = len(rf.log) + rf.SnapshotIndex
			
			match := make([]int, len(rf.peers))
			copy(match, rf.matchIndex)
			sort.Slice(match, func(i, j int) bool {
				return match[i] < match[j]
			})
			/*if match[len(match)-1] >= len(rf.log) {
				DFatalf("[%d] match wrong\n", rf.me)
			}*/
			/*for i, m := range rf.matchIndex {
				log.Printf("[%d] match %d\n",i, m)
			} */
			Index := match[(len(rf.peers)-1)/2]
			p := Index - rf.SnapshotIndex - 1
			//log.Printf("[%d] commit %d, tot_len = %d\n", rf.me, p, len(rf.log))
			Term := rf.SnapshotTerm
			if p >= 0 {
				Term = rf.log[p].Term
			}
			if Term == rf.currentTerm{
				//rf.commitIndex = max(rf.commitIndex, p)
				if rf.commitIndex < Index {
					rf.commitIndex = Index
					//DPrintf("[%d] leader become %d\n", rf.me, p)
				}
					
			}
		}()
		//DPrintf("[%d] distrocmd\n", rf.me)
	}
}