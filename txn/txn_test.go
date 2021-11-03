package txn

import (
	"testing"
	"time"
	"github.com/stretchr/testify/assert"
	"go-mvcc/index"
)

func TestNew(t *testing.T) {
	assert := assert.New(t)
	idx := index.MkIndex()
	txnMgr := MkTxnMgr()
	txn := txnMgr.New(idx)
	assert.Equal(len(txn.wset), 0)
}

func TestPutCommitAndGet(t *testing.T) {
	assert := assert.New(t)
	idx := index.MkIndex()
	txnMgr := MkTxnMgr()

	txnPut := txnMgr.New(idx)
	txnPut.Put(10, 20)
	txnPut.Put(11, 22)
	txnPut.Commit()

	txnGet := txnMgr.New(idx)
	v, found := txnGet.Get(10)
	assert.Equal(true, found)
	assert.Equal(uint64(20), v)

	v, found = txnGet.Get(11)
	assert.Equal(true, found)
	assert.Equal(uint64(22), v)

	_, found = txnGet.Get(12)
	assert.Equal(false, found)
	txnGet.Commit()
}

func TestPutAbortAndGet(t *testing.T) {
	assert := assert.New(t)
	idx := index.MkIndex()
	txnMgr := MkTxnMgr()

	txnPut := txnMgr.New(idx)
	txnPut.Put(10, 20)
	txnPut.Abort()

	txnGet := txnMgr.New(idx)
	_, found := txnGet.Get(10)
	assert.Equal(false, found)
	txnGet.Commit()
}

/**
 * Interleaved `txnPut.Put` and `txnGet.Get` with `txnPut.tid < txnGet.tid`.
 */
func TestInterleavedPutAndGet1(t *testing.T) {
	assert := assert.New(t)
	idx := index.MkIndex()
	txnMgr := MkTxnMgr()

	txnPut := txnMgr.New(idx)
	txnGet := txnMgr.New(idx)

	txnPut.Put(10, 20)

	go func() {
		time.Sleep(10 * time.Millisecond)
		txnPut.Commit()
	}()

	v, found := txnGet.Get(10)
	assert.Equal(true, found)
	assert.Equal(uint64(20), v)

	txnGet.Commit()
}

/**
 * Interleaved `txnPut.Put` and `txnGet.Get` with `txnPut.tid > txnGet.tid`.
 */
func TestInterleavedPutAndGet2(t *testing.T) {
	assert := assert.New(t)
	idx := index.MkIndex()
	txnMgr := MkTxnMgr()

	txnGet := txnMgr.New(idx)
	txnPut := txnMgr.New(idx)

	txnPut.Put(10, 20)

	go func() {
		time.Sleep(10 * time.Millisecond)
		txnPut.Commit()
	}()

	_, found := txnGet.Get(10)
	assert.Equal(false, found)

	txnGet.Commit()
}

func TestInOrderPuts(t *testing.T) {
	assert := assert.New(t)
	idx := index.MkIndex()
	txnMgr := MkTxnMgr()

	txnA := txnMgr.New(idx)
	txnB := txnMgr.New(idx)

	txnA.Put(10, 20)
	txnA.Commit()

	ok := txnB.Put(10, 200)
	assert.Equal(true, ok)
	txnB.Commit()

	txnGet := txnMgr.New(idx)
	v, found := txnGet.Get(10)
	assert.Equal(true, found)
	assert.Equal(uint64(200), v)
	txnGet.Commit()
}

/**
 * `Put` fails due to later writers.
 */
func TestFailedReversedPuts(t *testing.T) {
	assert := assert.New(t)
	idx := index.MkIndex()
	txnMgr := MkTxnMgr()

	txnA := txnMgr.New(idx)
	txnB := txnMgr.New(idx)

	txnB.Put(10, 20)
	txnB.Commit()

	ok := txnA.Put(10, 200)
	assert.Equal(false, ok)
	txnA.Abort()

	txnGet := txnMgr.New(idx)
	v, found := txnGet.Get(10)
	assert.Equal(true, found)
	assert.Equal(uint64(20), v)
	txnGet.Commit()
}

func TestInOrderGetAndPut(t *testing.T) {
	assert := assert.New(t)
	idx := index.MkIndex()
	txnMgr := MkTxnMgr()

	txnA := txnMgr.New(idx)
	txnB := txnMgr.New(idx)

	txnA.Get(10)
	txnA.Commit()

	ok := txnB.Put(10, 20)
	assert.Equal(true, ok)
	txnB.Commit()

	txnGet := txnMgr.New(idx)
	v, found := txnGet.Get(10)
	assert.Equal(true, found)
	assert.Equal(uint64(20), v)
	txnGet.Commit()
}

/**
 * `Put` fails due to later readers.
 */
func TestFailedReversedGetAndPut(t *testing.T) {
	assert := assert.New(t)
	idx := index.MkIndex()
	txnMgr := MkTxnMgr()

	txnA := txnMgr.New(idx)
	txnB := txnMgr.New(idx)

	txnB.Get(10)
	txnB.Commit()

	ok := txnA.Put(10, 200)
	assert.Equal(false, ok)
	txnA.Abort()

	txnGet := txnMgr.New(idx)
	_, found := txnGet.Get(10)
	assert.Equal(false, found)
	txnGet.Commit()
}

/**
 * `Put` fails due to concurrent writes (`txnA` writes first).
 */
func TestFailedConcurrentPuts1(t *testing.T) {
	assert := assert.New(t)
	idx := index.MkIndex()
	txnMgr := MkTxnMgr()

	txnA := txnMgr.New(idx)
	txnB := txnMgr.New(idx)

	txnA.Put(10, 20)

	ok := txnB.Put(10, 200)
	assert.Equal(false, ok)

	txnA.Commit()
	txnB.Abort()

	txnGet := txnMgr.New(idx)
	v, found := txnGet.Get(10)
	assert.Equal(true, found)
	assert.Equal(uint64(20), v)
	txnGet.Commit()
}

/**
 * `Put` fails due to concurrent writes (`txnB` writes first).
 */
func TestFailedConcurrentPuts2(t *testing.T) {
	assert := assert.New(t)
	idx := index.MkIndex()
	txnMgr := MkTxnMgr()

	txnA := txnMgr.New(idx)
	txnB := txnMgr.New(idx)

	txnB.Put(10, 20)

	ok := txnA.Put(10, 200)
	assert.Equal(false, ok)

	txnA.Abort()
	txnB.Commit()

	txnGet := txnMgr.New(idx)
	v, found := txnGet.Get(10)
	assert.Equal(true, found)
	assert.Equal(uint64(20), v)
	txnGet.Commit()
}

func TestReadMyWrite(t *testing.T) {
	assert := assert.New(t)
	idx := index.MkIndex()
	txnMgr := MkTxnMgr()

	txn := txnMgr.New(idx)
	txn.Put(10, 20)
	v, found := txn.Get(10)
	assert.Equal(true, found)
	assert.Equal(uint64(20), v)
}

func TestWriteMyWrite(t *testing.T) {
	assert := assert.New(t)
	idx := index.MkIndex()
	txnMgr := MkTxnMgr()

	txn := txnMgr.New(idx)
	txn.Put(10, 20)
	txn.Put(10, 200)
	v, found := txn.Get(10)
	assert.Equal(true, found)
	assert.Equal(uint64(200), v)

	txn.Commit()

	txnRd := txnMgr.New(idx)
	v, found = txnRd.Get(10)
	assert.Equal(true, found)
	assert.Equal(uint64(200), v)
}

