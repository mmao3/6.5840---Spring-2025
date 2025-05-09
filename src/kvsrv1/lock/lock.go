package lock

import (
	"6.5840/kvtest1"
	"6.5840/kvsrv1/rpc"
	"time"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockKey   string
	lockValue string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, lockKey: l}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	for {
		_, version, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey || (err == rpc.OK && version%2 == 0) {
			lk.lockValue = kvtest.RandValue(8)
			putErr := lk.ck.Put(lk.lockKey, lk.lockValue, version)

			if putErr == rpc.OK {
				// Successfully acquired lock
				return
			}

			if putErr == rpc.ErrMaybe {
				// Retry Get to confirm if we hold the lock
				val, newVersion := waitForGetOK(lk.ck, lk.lockKey)
				if newVersion != version && val == lk.lockValue {
					return // Assume lock is ours
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		val, version := waitForGetOK(lk.ck, lk.lockKey)

		// Only release if we still hold the lock
		if val != lk.lockValue {
			return // Another client owns the lock; skip release
		}

		putErr := lk.ck.Put(lk.lockKey, lk.lockValue, version)
		if putErr == rpc.OK {
			return // Released successfully
		}
		if putErr == rpc.ErrMaybe {
			_, newVersion := waitForGetOK(lk.ck, lk.lockKey)
			if newVersion == version+1 {
				return // Probably released
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// waitForGetOK repeatedly calls Get until it returns OK
func waitForGetOK(ck kvtest.IKVClerk, key string) (string, rpc.Tversion) {
	for {
		val, ver, err := ck.Get(key)
		if err == rpc.OK {
			return val, ver
		}
		time.Sleep(50 * time.Millisecond)
	}
}
