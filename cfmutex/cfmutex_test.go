package cfmutex

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

func TestCacheAligned(t *testing.T) {
	assert := assert.New(t)
	var cfmutex CFMutex
	size := unsafe.Sizeof(cfmutex)
	fmt.Printf("size = %d.\n", size)
	assert.Equal(uintptr(64), size)
}
