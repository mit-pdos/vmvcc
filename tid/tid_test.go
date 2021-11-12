package tid

import (
	"testing"
	"fmt"
	"github.com/stretchr/testify/assert"
	"go-mvcc/config"
)

func TestGetTID(t *testing.T) {
	assert := assert.New(t)
	tid := GetTID(0)
	fmt.Printf("tid = %d.\n", tid)
	assert.Equal(uint64(0), tid & (config.MAX_TXN - 1))

	tid = GetTID(13)
	fmt.Printf("tid = %d.\n", tid)
	assert.Equal(uint64(13), tid & (config.MAX_TXN - 1))
}

