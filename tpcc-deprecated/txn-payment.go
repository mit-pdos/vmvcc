package tpcc

import (
	"github.com/mit-pdos/go-mvcc/txn"
)

func payment(txn *txn.Txn) bool {
	return true
}

func TxnPayment(t *txn.Txn) bool {
	body := func(txn *txn.Txn) bool {
		return payment(txn)
	}
	ok := t.DoTxn(body)
	return ok
}
