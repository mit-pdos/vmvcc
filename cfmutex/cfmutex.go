package cfmutex

import (
	"sync"
)

type CFMutex struct {
	mutex   sync.Mutex
	padding [7]uint64
}

func (cfmutex *CFMutex) Lock() {
	cfmutex.mutex.Lock()
}

func (cfmutex *CFMutex) Unlock() {
	cfmutex.mutex.Unlock()
}

