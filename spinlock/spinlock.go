package spinlock

import (
	"sync/atomic"
)

type SpinLock struct {
	word	uint64
}

func (lock *SpinLock) Lock() {
	for !atomic.CompareAndSwapUint64(&lock.word, 0, 1) {
	}
}

func (lock *SpinLock) Unlock() {
	lock.word = 0
}

