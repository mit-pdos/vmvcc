package tuple

import (
	"testing"
	"sync"
	"time"
	"github.com/stretchr/testify/assert"
)

func TestMkTuple(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()
	assert.Equal(uint64(0), tuple.tidown)
	assert.Equal(uint64(0), tuple.tidrd)
	assert.Equal(0, len(tuple.vers))

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
		tuple.vers = append(tuple.vers, Version{0, 0, 0})
		tuple.rcond.Signal()
		tuple.latch.Unlock()
		wg.Done()
	}()

	wg.Wait()

	assert.Equal(2, len(tuple.vers))
	assert.Equal(Version{0, 0, 0}, tuple.vers[0])
	assert.Equal(Version{1, 1, 1}, tuple.vers[1])
}

func TestOwnAppendRead(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()
	tid := uint64(10)
	tuple.Own(tid)

	assert.Equal(tid, tuple.tidown)

	tuple.AppendVersion(tid, 20)

	assert.Equal(uint64(0), tuple.tidown)
	assert.Equal(tid, tuple.tidwr)
	assert.Equal(1, len(tuple.vers))
	assert.Equal(tid, tuple.vers[0].begin)
	assert.Equal(uint64(18446744073709551615), tuple.vers[0].end)
	assert.Equal(uint64(20), tuple.vers[0].val)

	v, ok := tuple.ReadVersion(tid)
	assert.Equal(uint64(20), v)
	assert.Equal(true, ok)
}

func TestOwnFreeRead(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()
	tid := uint64(10)
	tidrdPrev := tuple.tidrd
	tuple.Own(tid)

	assert.Equal(tid, tuple.tidown)

	tuple.Free(tid)

	assert.Equal(uint64(0), tuple.tidown)
	assert.Equal(tidrdPrev, tuple.tidrd)
	assert.Equal(0, len(tuple.vers))

	_, ok := tuple.ReadVersion(tid)
	assert.Equal(false, ok)
}

func TestReadNonexist(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()
	tid := uint64(10)

	_, ok := tuple.ReadVersion(tid)
	assert.Equal(false, ok)
}

func TestMultiVersion(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()

	tid := uint64(10)
	tuple.Own(tid)
	tuple.AppendVersion(tid, 20)

	tid = uint64(15)
	tuple.Own(tid)
	tuple.AppendVersion(tid, 30)

	assert.Equal(uint64(0), tuple.tidown)
	assert.Equal(tid, tuple.tidwr)
	assert.Equal(2, len(tuple.vers))
	assert.Equal(uint64(10), tuple.vers[0].begin)
	assert.Equal(uint64(15), tuple.vers[0].end)
	assert.Equal(uint64(20), tuple.vers[0].val)
	assert.Equal(uint64(15), tuple.vers[1].begin)
	assert.Equal(uint64(18446744073709551615), tuple.vers[1].end)
	assert.Equal(uint64(30), tuple.vers[1].val)

	tid = uint64(9)
	_, ok := tuple.ReadVersion(tid)
	assert.Equal(false, ok)

	for tid = uint64(10); tid < uint64(14); tid++ {
		v, ok := tuple.ReadVersion(tid)
		assert.Equal(uint64(20), v)
		assert.Equal(true, ok)
	}
	for tid = uint64(15); tid < uint64(19); tid++ {
		v, ok := tuple.ReadVersion(tid)
		assert.Equal(uint64(30), v)
		assert.Equal(true, ok)
	}
}

/**
 * Own fails due to later writers.
 */
func TestFailedOwn1(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()

	tid := uint64(10)
	tuple.Own(tid)
	tuple.AppendVersion(tid, 20)

	tid = uint64(5)
	ok := tuple.Own(tid)
	assert.Equal(false, ok)
}

/**
 * Own fails due to later readers.
 */
func TestFailedOwn2(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()

	tid := uint64(10)
	tuple.Own(tid)
	tuple.AppendVersion(tid, 20)

	tid = uint64(15)
	tuple.ReadVersion(tid)

	tid = uint64(13)
	ok := tuple.Own(tid)
	assert.Equal(false, ok)

	tid = uint64(18)
	ok = tuple.Own(tid)
	assert.Equal(true, ok)
}

/**
 * Own fails due to another txn owning the same tuple.
 */
func TestFailedOwn3(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()

	tid := uint64(10)
	tuple.Own(tid)

	tid = uint64(15)
	ok := tuple.Own(tid)
	assert.Equal(false, ok)
}

/**
 * Read blocks when targeting a tuple that is empty AND owned by another txn
 * whose `tid` is greater than that of the reader.
 */
func TestReadBlockingEmpty1(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()

	tid := uint64(10)
	tuple.Own(tid)

	go func() {
		time.Sleep(10 * time.Millisecond)
		tuple.AppendVersion(tid, 20)
	}()

	tidRd := uint64(5)
	_, ok := tuple.ReadVersion(tidRd)
	assert.Equal(false, ok)
}

/**
 * Read blocks when targeting a tuple that is empty AND owned by another txn
 * whose `tid` is less than that of the reader.
 */
func TestReadBlockingEmpty2(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()

	tid := uint64(10)
	tuple.Own(tid)

	go func() {
		time.Sleep(10 * time.Millisecond)
		tuple.AppendVersion(tid, 20)
	}()

	tidRd := uint64(15)
	v, ok := tuple.ReadVersion(tidRd)
	assert.Equal(uint64(20), v)
	assert.Equal(true, ok)
}

/**
 * Read blocks when targeting a tuple that is owned by another txn whose `tid`
 * is greater than that of the reader.
 */
func TestReadBlocking1(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()

	tid := uint64(2)
	tuple.Own(tid)
	tuple.AppendVersion(tid, 4)

	tid = uint64(10)
	tuple.Own(tid)

	go func() {
		time.Sleep(10 * time.Millisecond)
		tuple.AppendVersion(tid, 20)
	}()

	tidRd := uint64(5)
	v, ok := tuple.ReadVersion(tidRd)
	assert.Equal(uint64(4), v)
	assert.Equal(true, ok)
}

/**
 * Read blocks when targeting a tuple that is owned by another txn whose `tid`
 * is less than that of the reader.
 */
func TestReadBlocking2(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()

	tid := uint64(2)
	tuple.Own(tid)
	tuple.AppendVersion(tid, 4)

	tid = uint64(10)
	tuple.Own(tid)

	go func() {
		time.Sleep(10 * time.Millisecond)
		tuple.AppendVersion(tid, 20)
	}()

	tidRd := uint64(15)
	v, ok := tuple.ReadVersion(tidRd)
	assert.Equal(uint64(20), v)
	assert.Equal(true, ok)
}

