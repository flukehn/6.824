package kvraft

import (
	//"6.824/raft"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Oper string
	Key string
	Value string
	ClientId int64
	Reqseq int32
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	clientid := args.ClientId
	reqseq := args.Reqseq
	op := Op{
		Oper: "Get",
		Key: args.Key,
		ClientId: clientid,
		Reqseq: reqseq,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader{
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	waitch, ok := kv.waitch[index]
	if !ok {
		waitch = make(chan ExecResult)
		kv.waitch[index] = waitch
	}
	kv.mu.Unlock()
	select {
	case v:=<-waitch:
		if v.ClientId != clientid || v.Reqseq != reqseq {
			reply.Err=ErrWrongLeader
		} else {
			reply.Err=v.Err
			reply.Value=v.Value
		}
	case <-time.After(1000*time.Millisecond):
		reply.Err=ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	clientid := args.ClientId
	reqseq := args.Reqseq
	kv.mu.Lock()
	lastseq, ok := kv.lastseq[clientid]
	kv.mu.Unlock()
	if ok && lastseq >= reqseq {
		reply.Err=OK
		return
	}
	op := Op{
		Oper: args.Op,
		Key: args.Key,
		Value: args.Value,
		ClientId: clientid,
		Reqseq: reqseq,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader{
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	waitch, ok := kv.waitch[index]
	if !ok {
		waitch = make(chan ExecResult)
		kv.waitch[index] = waitch
	}
	kv.mu.Unlock()
	select {
	case v:=<-waitch:
		if v.ClientId != clientid || v.Reqseq != reqseq {
			reply.Err=ErrWrongLeader
		} else {
			reply.Err=v.Err
		}
	case <-time.After(1000*time.Millisecond):
		reply.Err=ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.waitch, index)
	kv.mu.Unlock()
}