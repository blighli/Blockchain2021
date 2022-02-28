package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

var ID int = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu         sync.Mutex
	lastServer int
	version    int64
	ID         int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.ID = ID
	ID++
	// You'll have to add code here.
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

	args := GetArgs{key}

	// You will have to modify this function.
	for i := 0; ; i++ {
		reply := GetReply{}

		DPrintf("sending GET RPC to %d\n", (i+ck.lastServer)%len(ck.servers))
		ok := ck.servers[(i+ck.lastServer)%len(ck.servers)].Call("KVServer.Get", &args, &reply)

		if !ok {
			continue
		}

		switch reply.Err {
		case OK:
			ck.lastServer = (i + ck.lastServer) % len(ck.servers)
			DPrintf("finish GET\n")
			return reply.Value
		case ErrNoKey:
			ck.lastServer = (i + ck.lastServer) % len(ck.servers)
			return ""
		case ErrWrongLeader:
			if i%len(ck.servers) == 0 {
				DPrintf("clerk get: traversed all servers but no one available.\n")
				time.Sleep(100 * time.Millisecond)
			}
			continue
		}

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
	args := PutAppendArgs{key, value, op, ck.version, ck.ID}
	ck.version++

	for i := 0; ; i++ {
		reply := PutAppendReply{}

		DPrintf("client %d sending PutAppend RPC to %d\n", ck.ID, (i+ck.lastServer)%len(ck.servers))
		DPrintf("putappend value is %s,%s", key, value)
		ok := ck.servers[(i+ck.lastServer)%len(ck.servers)].Call("KVServer.PutAppend", &args, &reply)

		if !ok {
			continue
		}

		switch reply.Err {
		case OK:
			ck.lastServer = (i + ck.lastServer) % len(ck.servers)
			DPrintf("set ck.lastServer to %d", ck.lastServer)
			DPrintf("finish PUTAPPEND\n")
			return
		case ErrNoKey:
			ck.lastServer = (i + ck.lastServer) % len(ck.servers)
			fmt.Printf("PutAppend: no key\n")
			DPrintf("set ck.lastServer to %d", ck.lastServer)
			return
		case ErrWrongLeader:
			if i != 0 && i%len(ck.servers) == 0 {
				DPrintf("clerk put: traversed all servers but no one available.\n")
				time.Sleep(100 * time.Millisecond)
			}
			continue
		}

	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
