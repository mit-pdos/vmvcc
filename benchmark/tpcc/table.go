package tpcc

import (
	"github.com/mit-pdos/go-mvcc/txn"
)

type Table interface {
	gkey() uint64
	encode() string
	decode(opaque string)
}

/**
 * Reader and writer operation invoking transation methods.
 */
func TableRead(tbl Table, txn *txn.Txn) bool {
	gkey := tbl.gkey()
	opaque, found := txn.Get(gkey)
	/* TODO: check if we really need to do this check. */
	if !found {
		return false
	}
	tbl.decode(opaque)
	return true
}

func TableWrite(tbl Table, txn *txn.Txn) {
	gkey := tbl.gkey()
	s := tbl.encode()
	txn.Put(gkey, s)
}
