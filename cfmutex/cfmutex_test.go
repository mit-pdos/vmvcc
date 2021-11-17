package cfmutex

import (
	"fmt"
	"testing"
	"unsafe"
	"github.com/stretchr/testify/assert"
)

func TestCacheAligned(t *testing.T) {
	assert := assert.New(t)
	var cfmutex CFMutex
	size := unsafe.Sizeof(cfmutex)
	fmt.Printf("size = %d.\n", size)
	assert.Equal(uintptr(64), size)
}

