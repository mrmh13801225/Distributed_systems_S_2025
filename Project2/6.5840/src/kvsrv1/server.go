package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex
	kvStore      map[string]string
	versionStore map[string]rpc.Tversion
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		mu:           sync.Mutex{},
		kvStore:      make(map[string]string),
		versionStore: make(map[string]rpc.Tversion),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	if value, ok := kv.kvStore[args.Key]; ok {
		reply.Value = value
		reply.Version = kv.versionStore[args.Key]
		reply.Err = rpc.OK
		DPrintf("Server: Get key: %s value: %s with version %d", args.Key, value, reply.Version)
	} else {
		reply.Err = rpc.ErrNoKey
		DPrintf("Server: Get key: %s not found", args.Key)
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
