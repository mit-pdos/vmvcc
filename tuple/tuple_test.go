package tuple

import (
	"testing"
	"sync"
	"github.com/stretchr/testify/assert"
)

func TestMkTuple(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()
	assert.Equal(tuple.tidwr, uint64(0))
	assert.Equal(tuple.tidlast, uint64(0))
	assert.Equal(len(tuple.vers), 0)

	wg := new(sync.WaitGroup)
	wg.Add(2)

	go func() {
		tuple.latch.Lock()
		for len(tuple.vers) != 1 {
			tuple.rcond.Wait()
		}
		tuple.vers = append(tuple.vers, Version{1, 1, 1})
		tuple.latch.Unlock()
		wg.Done()
	}()

	go func() {
		tuple.latch.Lock()
		for len(tuple.vers) != 0 {
			tuple.rcond.Wait()
		}
		tuple.vers = append(tuple.vers, Version{0, 0, 0})
		tuple.latch.Unlock()
		wg.Done()
	}()

	wg.Wait()

	assert.Equal(len(tuple.vers), 2)
	assert.Equal(tuple.vers[0], Version{0, 0, 0})
	assert.Equal(tuple.vers[1], Version{1, 1, 1})
}
