package txn

import (
	"go-mvcc/tuple"
	"go-mvcc/index"
)

type WrEnt struct {
	val		uint64
	tuple	*tuple.Tuple
}

type Txn struct {
	tid		uint64
	wset	[]WrEnt
	idx		*index.Index
}

func (txn *Txn) Put(key, val uint64) bool {
	idx := txn.idx
	tuple := idx.GetTuple(key)

	/* Try to get the permission to update this tuple. */
	ok := tuple.Own(txn.tid)
	if !ok {
		return false
	}

	/* Add the key-value pair to the local write set. */
	txn.wset = append(txn.wset, WrEnt{val: val, tuple: tuple})

	return true
}

func (txn *Txn) Get(key uint64) (uint64, bool) {
	idx := txn.idx
	tuple := idx.GetTuple(key)
	val, found := tuple.ReadVersion(txn.tid)
	return val, found
}

func (txn *Txn) Commit() {
	for _, wrent := range txn.wset {
		val := wrent.val
		tuple := wrent.tuple
		tuple.AppendVersion(txn.tid, val)
	}
}

func (txn *Txn) Abort() {
	for _, wrent := range txn.wset {
		tuple := wrent.tuple
		tuple.Free(txn.tid)
	}
}

