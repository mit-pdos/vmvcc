package examples

import (
	"github.com/mit-pdos/go-mvcc/txn"
)

func IncrementSeq(txn *txn.Txn, p *uint64) bool {
	v, _ := txn.Get(0)
	if v == 18446744073709551615 {
		return false
	}
	txn.Put(0, v + 1)

	return true
}

func Increment(t *txn.Txn, p *uint64) bool {
	body := func(txn *txn.Txn) bool {
		return IncrementSeq(txn, p)
	}
	return t.DoTxn(body)
}

func DecrementSeq(txn *txn.Txn, p *uint64) bool {
	v, _ := txn.Get(0)
	if v == 0 {
		return false
	}
	txn.Put(0, v - 1)
	return true
}

func Decrement(t *txn.Txn, p *uint64) bool {
	body := func(txn *txn.Txn) bool {
		return DecrementSeq(txn, p)
	}
	return t.DoTxn(body)
}

func InitializeCounterData(mgr *txn.TxnMgr) {
	// TODO: Initialize key 0 to some value
}

func InitCounter() *txn.TxnMgr {
	mgr := txn.MkTxnMgr()
	InitializeCounterData(mgr)
	return mgr
}

func CallIncrement(mgr *txn.TxnMgr) (uint64, bool) {
	txn := mgr.New()
	var n uint64
	ok := Increment(txn, &n)
	return n, ok
}

func CallIncrementTwice(mgr *txn.TxnMgr) (uint64, uint64, bool) {
	txn := mgr.New()
	var n1 uint64
	ok1 := Increment(txn, &n1)
	if !ok1 {
		return 0, 0, false
	}
	var n2 uint64
	ok2 := Increment(txn, &n2)
	return n1, n2, ok2
}

func CallDecrement(mgr *txn.TxnMgr) (uint64, bool) {
	txn := mgr.New()
	var n uint64
	ok := Decrement(txn, &n)
	return n, ok
}

