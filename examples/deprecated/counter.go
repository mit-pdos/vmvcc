//go:build ignore
package examples

import (
	"github.com/goose-lang/goose/machine"
	"github.com/mit-pdos/vmvcc/txn"
)

func fetch(txn *txn.Txn, p *uint64) bool {
	v, _ := txn.Read(0)
	*p = v

	return true
}

func Fetch(t *txn.Txn) uint64 {
	var n uint64
	body := func(txn *txn.Txn) bool {
		return fetch(txn, &n)
	}
	t.Run(body)
	return n
}

func increment(txn *txn.Txn, p *uint64) bool {
	v, _ := txn.Read(0)
	*p = v
	if v == 18446744073709551615 {
		return false
	}
	txn.Write(0, v+1)

	return true
}

func Increment(t *txn.Txn) (uint64, bool) {
	var n uint64
	body := func(txn *txn.Txn) bool {
		return increment(txn, &n)
	}
	ok := t.Run(body)
	return n, ok
}

func decrement(txn *txn.Txn, p *uint64) bool {
	v, _ := txn.Read(0)
	*p = v
	if v == 0 {
		return false
	}
	txn.Write(0, v-1)
	return true
}

func Decrement(t *txn.Txn) (uint64, bool) {
	var n uint64
	body := func(txn *txn.Txn) bool {
		return decrement(txn, &n)
	}
	ok := t.Run(body)
	return n, ok
}

func InitializeCounterData(mgr *txn.TxnMgr) {
	// Initialize key 0 to some value
	body := func(txn *txn.Txn) bool {
		txn.Write(0, 0)
		return true
	}
	// We wrap this transaction in a loop because the spec says it might fail.
	// However, init methods should never fail as there are no contending txns.
	// A better way is to have init RPs (for `tuple` and `table`) and specs
	// that always succeed given those init RPs. These init RPs are only
	// available at init time, and updated to regular RPs before they are
	// sealed in some invariants.
	t := mgr.New()
	for !t.Run(body) {
	}
}

func InitCounter() *txn.TxnMgr {
	mgr := txn.MkTxnMgr()
	InitializeCounterData(mgr)
	return mgr
}

func CallIncrement(mgr *txn.TxnMgr) {
	txn := mgr.New()
	Increment(txn)
}

func CallIncrementFetch(mgr *txn.TxnMgr) {
	txn := mgr.New()
	n1, ok1 := Increment(txn)
	if !ok1 {
		return
	}
	n2 := Fetch(txn)
	machine.Assert(n1 < n2)
}

func CallDecrement(mgr *txn.TxnMgr) {
	txn := mgr.New()
	Decrement(txn)
}
