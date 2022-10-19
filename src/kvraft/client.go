package kvraft

import (
	"6.824/labrpc"
	crypto_rand "crypto/rand"
	"math/big"
	"math/rand"
	"sync/atomic"
	//"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastleader int32
	clientid int64
	reqseq int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crypto_rand.Int(crypto_rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastleader = rand.Int31n(int32(len(servers)))
	ck.clientid = nrand()
	ck.reqseq = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	reqseq := atomic.AddInt32(&ck.reqseq, 1)
	for {
		leader_now := atomic.LoadInt32(&ck.lastleader)
		args := GetArgs{
			Key: key,
			ClientId: ck.clientid,
			Reqseq: reqseq,
		}
		reply := GetReply{}
		ok := ck.servers[leader_now].Call("KVServer.Get", &args, &reply)
		if ok{
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			}
		}
		atomic.StoreInt32(&ck.lastleader, rand.Int31n(int32(len(ck.servers))))
		DPrintf("Get client=%d reqseq=%d key=%s err=%s\n", ck.clientid, reqseq, key, reply.Err)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	reqseq := atomic.AddInt32(&ck.reqseq, 1)
	for {
		leader_now := atomic.LoadInt32(&ck.lastleader)
		args := PutAppendArgs{
			Key: key,
			Value: value,
			Op: op,
			ClientId: ck.clientid,
			Reqseq: reqseq,
		}
		reply := PutAppendReply{}
		DPrintf("%s client=%d reqseq=%d key=%s value=%s\n", op, ck.clientid, reqseq, key, value)
		ok := ck.servers[leader_now].Call("KVServer.PutAppend", &args, &reply)
		if ok{
			if reply.Err == OK {
				return
			}else if reply.Err == ErrNoKey{
				return
			}
		}
		atomic.StoreInt32(&ck.lastleader, rand.Int31n(int32(len(ck.servers))))
		DPrintf("%s client=%d err=%s\n", op, ck.clientid, reply.Err)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
