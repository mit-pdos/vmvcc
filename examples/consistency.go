package examples

import (
	"github.com/mit-pdos/go-mvcc/txn"
)

func Example1(txn *txn.Txn) uint64 {
	txn.Begin()
	v0, _ := txn.Get(0)
	v2, _ := txn.Get(2)
	total := v0 + v2
	return total
}

