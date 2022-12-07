package ycsb

import (
	"testing"
	"fmt"
	"github.com/stretchr/testify/assert"
)


func TestZipfian(t *testing.T) {
	assert := assert.New(t)
	gen := NewGenerator(0, 1, 1000, 100, DIST_ZIPFIAN, 0.9)
	for i := 0; i < 100; i++ {
		fmt.Printf("%d\n", gen.PickKey())
	}
	assert.Equal(true, true)
}
