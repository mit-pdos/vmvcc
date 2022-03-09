package tuple

import (
	"testing"
	"time"
	"github.com/stretchr/testify/assert"
	"github.com/mit-pdos/go-mvcc/common"
)

func TestMkTuple(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()
	assert.Equal(uint64(0), tuple.tidown)
	assert.Equal(uint64(0), tuple.tidlast)
	assert.Equal(1, len(tuple.vers))
}

func TestOwnAppendRead(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()

	tid := uint64(10)
	tuple.Own(tid)

	assert.Equal(tid, tuple.tidown)

	tuple.AppendVersion(tid, 20)

	assert.Equal(uint64(0), tuple.tidown)

	assert.Equal(2, len(tuple.vers))
	assert.Equal(uint64(10), tuple.vers[1].begin)

	_, ret := tuple.ReadVersion(tid - 1)
	assert.Equal(common.RET_NONEXIST, ret)

	_, ret = tuple.ReadVersion(tid)
	assert.Equal(common.RET_NONEXIST, ret)

	v, ret := tuple.ReadVersion(tid + 1)
	assert.Equal(uint64(20), v)
	assert.Equal(common.RET_SUCCESS, ret)
}

func TestOwnFreeRead(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()

	tid := uint64(10)
	tidlastPrev := tuple.tidlast

	tuple.Own(tid)

	assert.Equal(tid, tuple.tidown)

	tuple.Free(tid)

	assert.Equal(uint64(0), tuple.tidown)
	/* `tidlast` remains unchanged. */
	assert.Equal(tidlastPrev, tuple.tidlast)
	assert.Equal(1, len(tuple.vers))

	_, ret := tuple.ReadVersion(tid - 1)
	assert.Equal(common.RET_NONEXIST, ret)

	_, ret = tuple.ReadVersion(tid)
	assert.Equal(common.RET_NONEXIST, ret)

	_, ret = tuple.ReadVersion(tid + 1)
	assert.Equal(common.RET_NONEXIST, ret)
}

func TestReadNonexist(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()

	tid := uint64(10)
	_, ret := tuple.ReadVersion(tid)
	assert.Equal(common.RET_NONEXIST, ret)
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
	assert.Equal(3, len(tuple.vers))

	assert.Equal(uint64(10), tuple.vers[1].begin)
	assert.Equal(uint64(20), tuple.vers[1].val)

	assert.Equal(uint64(15), tuple.vers[2].begin)
	assert.Equal(uint64(30), tuple.vers[2].val)

	_, ret := tuple.ReadVersion(9)
	assert.Equal(common.RET_NONEXIST, ret)

	_, ret = tuple.ReadVersion(10)
	assert.Equal(common.RET_NONEXIST, ret)

	v, ret := tuple.ReadVersion(11)
	assert.Equal(common.RET_SUCCESS, ret)
	assert.Equal(uint64(20), v)

	v, ret = tuple.ReadVersion(14)
	assert.Equal(common.RET_SUCCESS, ret)
	assert.Equal(uint64(20), v)

	v, ret = tuple.ReadVersion(15)
	assert.Equal(common.RET_SUCCESS, ret)
	assert.Equal(uint64(20), v)

	v, ret = tuple.ReadVersion(16)
	assert.Equal(common.RET_SUCCESS, ret)
	assert.Equal(uint64(30), v)
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
	ret := tuple.Own(tid)
	assert.Equal(common.RET_UNSERIALIZABLE, ret)

	assert.Equal(uint64(0), tuple.tidown)
	assert.Equal(uint64(10), tuple.tidlast)
	assert.Equal(2, len(tuple.vers))
	assert.Equal(uint64(10), tuple.vers[1].begin)
	assert.Equal(uint64(20), tuple.vers[1].val)
}

/**
 * Own fails due to later readers.
 */
func TestFailedOwn2(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()

	tid := uint64(10)
	tuple.ReadVersion(tid)

	tid = uint64(5)
	ret := tuple.Own(tid)
	assert.Equal(common.RET_UNSERIALIZABLE, ret)

	assert.Equal(uint64(0), tuple.tidown)
	assert.Equal(uint64(10), tuple.tidlast)
	assert.Equal(1, len(tuple.vers))

	tid = uint64(15)
	ret = tuple.Own(tid)
	assert.Equal(common.RET_SUCCESS, ret)

	assert.Equal(tid, tuple.tidown)
	assert.Equal(uint64(10), tuple.tidlast)
	assert.Equal(1, len(tuple.vers))
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
	ret := tuple.Own(tid)
	assert.Equal(common.RET_RETRY, ret)

	assert.Equal(uint64(10), tuple.tidown)
	assert.Equal(uint64(0), tuple.tidlast)
	assert.Equal(1, len(tuple.vers))
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
	_, ret := tuple.ReadVersion(tidRd)
	assert.Equal(common.RET_NONEXIST, ret)
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
	v, ret := tuple.ReadVersion(tidRd)
	assert.Equal(uint64(20), v)
	assert.Equal(common.RET_SUCCESS, ret)
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
	v, ret := tuple.ReadVersion(tidRd)
	assert.Equal(uint64(4), v)
	assert.Equal(common.RET_SUCCESS, ret)
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
	v, ret := tuple.ReadVersion(tidRd)
	assert.Equal(uint64(20), v)
	assert.Equal(common.RET_SUCCESS, ret)
}

func TestRemoveVersions(t *testing.T) {
	assert := assert.New(t)
	tuple := MkTuple()

	tidA := uint64(10)
	tidB := uint64(20)

	tuple.Own(tidA)
	tuple.AppendVersion(tidA, 100)
	assert.Equal(2, len(tuple.vers))

	tuple.RemoveVersions(20)
	assert.Equal(1, len(tuple.vers))

	tuple.Own(tidB)
	tuple.AppendVersion(tidB, 200)
	assert.Equal(2, len(tuple.vers))

	tidRd := uint64(15)
	v, ret := tuple.ReadVersion(tidRd)
	assert.Equal(uint64(100), v)
	assert.Equal(common.RET_SUCCESS, ret)

	tuple.RemoveVersions(19)
	assert.Equal(2, len(tuple.vers))
	tuple.RemoveVersions(20)
	assert.Equal(2, len(tuple.vers))
	tuple.RemoveVersions(21)
	assert.Equal(1, len(tuple.vers))
}

