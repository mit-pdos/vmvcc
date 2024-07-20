package vmvcc

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"sync"
	"testing"
)

func TestReadRead(t *testing.T) {
	assert := assert.New(t)
	db := MkDB()
	txno := db.NewTxn()
	body := func(txni *Txn) bool {
		_, found := txni.Read(0)
		assert.Equal(false, found)
		assert.Equal(uint32(1), db.idx.GetTuple(0).lock)
		_, found = txni.Read(0)
		assert.Equal(false, found)
		assert.Equal(uint32(1), db.idx.GetTuple(0).lock)
		return true
	}
	ok := txno.Run(body)
	assert.Equal(true, ok)
	assert.Equal(uint32(0), db.idx.GetTuple(0).lock)
	assert.Equal(true, db.idx.GetTuple(0).del)
}

func TestReadWriteCommit(t *testing.T) {
	assert := assert.New(t)
	db := MkDB()
	txno := db.NewTxn()
	body := func(txni *Txn) bool {
		_, found := txni.Read(0)
		assert.Equal(false, found)
		assert.Equal(uint32(1), db.idx.GetTuple(0).lock)
		txni.Write(0, "hello")
		/* `lock` should still be 1 before commit. */
		assert.Equal(uint32(1), db.idx.GetTuple(0).lock)
		return true
	}
	ok := txno.Run(body)
	assert.Equal(true, ok)
	assert.Equal(uint32(0), db.idx.GetTuple(0).lock)
	assert.Equal(false, db.idx.GetTuple(0).del)
	assert.Equal("hello", db.idx.GetTuple(0).val)
}

func TestReadWriteAbort(t *testing.T) {
	assert := assert.New(t)
	db := MkDB()
	txno := db.NewTxn()
	body := func(txni *Txn) bool {
		_, found := txni.Read(0)
		assert.Equal(false, found)
		assert.Equal(uint32(1), db.idx.GetTuple(0).lock)
		txni.Write(0, "hello")
		/* `lock` should still be 1 before commit. */
		assert.Equal(uint32(1), db.idx.GetTuple(0).lock)
		return false
	}
	ok := txno.Run(body)
	assert.Equal(false, ok)
	assert.Equal(uint32(0), db.idx.GetTuple(0).lock)
	assert.Equal(true, db.idx.GetTuple(0).del)
}

func TestWriteReadCommit(t *testing.T) {
	assert := assert.New(t)
	db := MkDB()
	txno := db.NewTxn()
	body := func(txni *Txn) bool {
		txni.Write(0, "hello")
		/* `lock` should still be 0 before commit. */
		assert.Equal(uint32(0), db.idx.GetTuple(0).lock)
		v, found := txni.Read(0)
		assert.Equal(true, found)
		assert.Equal("hello", v)
		/* write set hit, not even acquire read lock */
		assert.Equal(uint32(0), db.idx.GetTuple(0).lock)
		return true
	}
	ok := txno.Run(body)
	assert.Equal(true, ok)
	assert.Equal(uint32(0), db.idx.GetTuple(0).lock)
	assert.Equal(false, db.idx.GetTuple(0).del)
	assert.Equal("hello", db.idx.GetTuple(0).val)
}

func TestWriteReadAbort(t *testing.T) {
	assert := assert.New(t)
	db := MkDB()
	txno := db.NewTxn()
	body := func(txni *Txn) bool {
		txni.Write(0, "hello")
		/* `lock` should still be 0 before commit. */
		assert.Equal(uint32(0), db.idx.GetTuple(0).lock)
		v, found := txni.Read(0)
		assert.Equal(true, found)
		assert.Equal("hello", v)
		/* write set hit, not even acquire read lock */
		assert.Equal(uint32(0), db.idx.GetTuple(0).lock)
		return false
	}
	ok := txno.Run(body)
	assert.Equal(false, ok)
	assert.Equal(uint32(0), db.idx.GetTuple(0).lock)
	assert.Equal(true, db.idx.GetTuple(0).del)
}

func worker(i int, txno *Txn) {
	t := 0
	c := 0
	rd := rand.New(rand.NewSource(int64(i)))
	body := func(txni *Txn) bool {
		for i := 0; i < 5; i++ {
			key := rd.Uint64() % 16
			if rd.Uint64()%2 == 0 {
				txni.Read(key)
			} else {
				txni.Write(key, "hello")
			}
		}
		return true
	}
	for j := 0; j < 10000; j++ {
		ok := txno.Run(body)
		if ok {
			c++
		}
		t++
	}
	fmt.Printf("Thread %d : (%d / %d)\n", i, c, t)
}

func TestStress(t *testing.T) {
	assert := assert.New(t)
	db := MkDB()

	/* Initialize each key to 0. */
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		txno := db.NewTxn()
		wg.Add(1)
		go func(x int) {
			defer wg.Done()
			worker(x, txno)
		}(i)
	}
	wg.Wait()

	var key uint64
	for key = 0; key < 16; key++ {
		assert.Equal(uint32(0), db.idx.GetTuple(key).lock)
	}
}
