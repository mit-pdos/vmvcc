package tpcc

import (
	"github.com/mit-pdos/vmvcc/txn"
)

/**
 * We could have also added `gkey` as method of `record`.
 * However, the problem with this interface is that retrieving records with
 * index won't fit this.
 */
type record interface {
	encode() string
	decode(opaque string)
}

func readtbl(txn *txn.Txn, gkey uint64, r record) bool {
	opaque, found := txn.Read(gkey)
	if !found {
		return false
	}
	r.decode(opaque)
	return true
}

func writetbl(txn *txn.Txn, gkey uint64, r record) {
	s := r.encode()
	txn.Write(gkey, s)
}

func deletetbl(txn *txn.Txn, gkey uint64) {
	txn.Delete(gkey)
}
