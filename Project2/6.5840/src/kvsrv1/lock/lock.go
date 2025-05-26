package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck      kvtest.IKVClerk
	key     string
	ownerID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	lk.key = l
	lk.ownerID = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	for {
		client, version, err := lk.ck.Get(lk.key)
		if err != rpc.OK {
			err = lk.ck.Put(lk.key, lk.ownerID, 0)
			if err != rpc.OK {
				continue
			}
			return
		} else {
			if lk.ownerID == client {
				return
			} else if client == "" {
				err = lk.ck.Put(lk.key, lk.ownerID, version)
				if err != rpc.OK {
					continue
				}
				return
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

func (lk *Lock) Release() {
	for {
		client, version, err := lk.ck.Get(lk.key)
		if err != rpc.OK {
			return
		}

		if client == lk.ownerID {
			err = lk.ck.Put(lk.key, "", version)
			if err != rpc.OK {
				continue
			}
			return
		} else {
			return
		}
	}
}
