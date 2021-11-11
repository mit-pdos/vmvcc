package txn

import (
	"go-mvcc/tuple"
	"go-mvcc/index"
)

/**
 * We need `key` as to match in the local write set
 */
type WrEnt struct {
	key		uint64
	val		uint64
	tuple	*tuple.Tuple
}

/**
 * `wset` as a `map[uint64]WrEnt` has the issue of allocating and deallocating
 * many `WrEnt`.
 */
type Txn struct {
	tid		uint64
	wset	[]WrEnt
	idx		*index.Index
	txnMgr	*TxnMgr
}

func (txn *Txn) Put(key, val uint64) bool {
	/* First try to find `key` in the local write set. */
	var found bool = false
	for i, _ := range txn.wset {
		if key == txn.wset[i].key {
			went := &txn.wset[i]
			went.val = val
			found = true
		}
	}
	if found {
		return true
	}

	idx := txn.idx
	tuple := idx.GetTuple(key)

	/* Try to get the permission to update this tuple. */
	ok := tuple.Own(txn.tid)
	if !ok {
		return false
	}

	/* Add the key-value pair to the local write set. */
	txn.wset = append(txn.wset, WrEnt{key: key, val: val, tuple: tuple})

	return true
}

func (txn *Txn) Get(key uint64) (uint64, bool) {
	/* First try to find `key` in the local write set. */
	var found bool = false
	var val uint64 = 0
	for i, _ := range txn.wset {
		if key == txn.wset[i].key {
			went := &txn.wset[i]
			val = went.val
			found = true
		}
	}
	if found {
		return val, true
	}

	idx := txn.idx
	tuple := idx.GetTuple(key)
	/* Cannot reuse `found` and `val` as Goose forbids multi-assignment. */
	valTuple, foundTuple := tuple.ReadVersion(txn.tid)
	return valTuple, foundTuple
}

func (txn *Txn) Begin() {
	tid := txn.txnMgr.activate()
	txn.tid = tid
	txn.wset = txn.wset[:0]
}

func (txn *Txn) Commit() {
	for _, wrent := range txn.wset {
		val := wrent.val
		tuple := wrent.tuple
		tuple.AppendVersion(txn.tid, val)
	}
	txn.txnMgr.deactivate(txn.tid)
}

func (txn *Txn) Abort() {
	for _, wrent := range txn.wset {
		tuple := wrent.tuple
		tuple.Free(txn.tid)
	}
	txn.txnMgr.deactivate(txn.tid)
}

