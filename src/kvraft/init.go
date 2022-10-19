package kvraft
import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

func (kv* KVServer) Execmd() {
	for msg := range kv.applyCh {
		kv.mu.Lock()
		if msg.CommandValid {
			DPrintf("[%d] Exec command=%v\n", kv.me, msg.Command)
			op := msg.Command.(Op)
			clientid := op.ClientId
			reqseq := op.Reqseq
			ret := ExecResult{
				ClientId: clientid,
				Reqseq: reqseq,
			}
			if op.Oper == "Get" {
				Value, exist := kv.db[op.Key]
				if !exist{
					ret.Err=ErrNoKey
				}else{
					ret.Err=OK
					ret.Value=Value
				}
			} else {
				kv_client_seq, exist := kv.lastseq[clientid]
				if exist && kv_client_seq >= reqseq {
					ret.Err=OK
				} else {
					ret.Err=OK
					kv.lastseq[clientid] = reqseq
					if op.Oper == "Put" {
						kv.db[op.Key] = op.Value
					} else {
						now, exist := kv.db[op.Key]
						if !exist {
							kv.db[op.Key] = op.Value	
						} else {
							kv.db[op.Key] = now + op.Value
						}
					}
				}
			}
			index := msg.CommandIndex
			waitch, exist := kv.waitch[index]
			go func() {
				if exist {
					waitch <- ret
				}
			}()
		} else if msg.SnapshotValid {
			DPrintf("Snapshot not impl yet.\n")
		}
		kv.mu.Unlock()
	}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(raft.ApplyMsg{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.waitch = make(map[int]chan ExecResult)
	kv.lastseq = make(map[int64]int32)
	go kv.Execmd()
	return kv
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//