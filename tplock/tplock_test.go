package tplock

import (
	// "fmt"
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestReadRead(t *testing.T) {
	assert := assert.New(t)
	db := MkTxnMgr()
	txno := db.New()
	body := func(txni *Txn) bool {
		_, found := txni.Get(0)
		assert.Equal(false, found)
		assert.Equal(uint32(1), db.idx.GetTuple(0).lock)
		_, found = txni.Get(0)
		assert.Equal(false, found)
		assert.Equal(uint32(1), db.idx.GetTuple(0).lock)
		return true
	}
	ok := txno.DoTxn(body)
	assert.Equal(true, ok)
	assert.Equal(uint32(0), db.idx.GetTuple(0).lock)
	assert.Equal(true, db.idx.GetTuple(0).del)
}

func TestReadWriteCommit(t *testing.T) {
	assert := assert.New(t)
	db := MkTxnMgr()
	txno := db.New()
	body := func(txni *Txn) bool {
		_, found := txni.Get(0)
		assert.Equal(false, found)
		assert.Equal(uint32(1), db.idx.GetTuple(0).lock)
		txni.Put(0, "hello")
		/* `lock` should still be 1 before commit. */
		assert.Equal(uint32(1), db.idx.GetTuple(0).lock)
		return true
	}
	ok := txno.DoTxn(body)
	assert.Equal(true, ok)
	assert.Equal(uint32(0), db.idx.GetTuple(0).lock)
	assert.Equal(false, db.idx.GetTuple(0).del)
	assert.Equal("hello", db.idx.GetTuple(0).val)
}

func TestReadWriteAbort(t *testing.T) {
	assert := assert.New(t)
	db := MkTxnMgr()
	txno := db.New()
	body := func(txni *Txn) bool {
		_, found := txni.Get(0)
		assert.Equal(false, found)
		assert.Equal(uint32(1), db.idx.GetTuple(0).lock)
		txni.Put(0, "hello")
		/* `lock` should still be 1 before commit. */
		assert.Equal(uint32(1), db.idx.GetTuple(0).lock)
		return false
	}
	ok := txno.DoTxn(body)
	assert.Equal(false, ok)
	assert.Equal(uint32(0), db.idx.GetTuple(0).lock)
	assert.Equal(true, db.idx.GetTuple(0).del)
}

func TestWriteReadCommit(t *testing.T) {
	assert := assert.New(t)
	db := MkTxnMgr()
	txno := db.New()
	body := func(txni *Txn) bool {
		txni.Put(0, "hello")
		/* `lock` should still be 0 before commit. */
		assert.Equal(uint32(0), db.idx.GetTuple(0).lock)
		v, found := txni.Get(0)
		assert.Equal(true, found)
		assert.Equal("hello", v)
		/* write set hit, not even acquire read lock */
		assert.Equal(uint32(0), db.idx.GetTuple(0).lock)
		return true
	}
	ok := txno.DoTxn(body)
	assert.Equal(true, ok)
	assert.Equal(uint32(0), db.idx.GetTuple(0).lock)
	assert.Equal(false, db.idx.GetTuple(0).del)
	assert.Equal("hello", db.idx.GetTuple(0).val)
}

func TestWriteReadAbort(t *testing.T) {
	assert := assert.New(t)
	db := MkTxnMgr()
	txno := db.New()
	body := func(txni *Txn) bool {
		txni.Put(0, "hello")
		/* `lock` should still be 0 before commit. */
		assert.Equal(uint32(0), db.idx.GetTuple(0).lock)
		v, found := txni.Get(0)
		assert.Equal(true, found)
		assert.Equal("hello", v)
		/* write set hit, not even acquire read lock */
		assert.Equal(uint32(0), db.idx.GetTuple(0).lock)
		return false
	}
	ok := txno.DoTxn(body)
	assert.Equal(false, ok)
	assert.Equal(uint32(0), db.idx.GetTuple(0).lock)
	assert.Equal(true, db.idx.GetTuple(0).del)
}
