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
	kv.mu.Lock()
	defer kv.mu.Unlock()

	currentVersion, keyExists := kv.versionStore[args.Key]

	if keyExists {
		// Handle existing key
		if err := kv.updateExistingKey(args, currentVersion); err != rpc.OK {
			reply.Err = err
			return
		}
	} else {
		// Handle new key
		if err := kv.createNewKey(args); err != rpc.OK {
			reply.Err = err
			return
		}
	}

	reply.Err = rpc.OK
}

// updateExistingKey handles updating an existing key-value pair
func (kv *KVServer) updateExistingKey(args *rpc.PutArgs, currentVersion rpc.Tversion) rpc.Err {
	if args.Version != currentVersion {
		DPrintf("Server: Put version mismatch for key: %s (expected: %d, got: %d)", 
			args.Key, currentVersion, args.Version)
		return rpc.ErrVersion
	}

	// Update value and increment version
	kv.kvStore[args.Key] = args.Value
	kv.versionStore[args.Key] = currentVersion + 1
	
	DPrintf("Server: Put updated key: %s to value: %s (version: %d -> %d)", 
		args.Key, args.Value, currentVersion, currentVersion+1)
	
	return rpc.OK
}

// createNewKey handles creating a new key-value pair
func (kv *KVServer) createNewKey(args *rpc.PutArgs) rpc.Err {
	if args.Version != 0 {
		DPrintf("Server: Put failed - key: %s doesn't exist, expected version 0, got: %d", 
			args.Key, args.Version)
		return rpc.ErrNoKey
	}

	// Create new key with version 1
	kv.kvStore[args.Key] = args.Value
	kv.versionStore[args.Key] = 1
	
	DPrintf("Server: Put created new key: %s with value: %s (version: 1)", 
		args.Key, args.Value)
	
	return rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
