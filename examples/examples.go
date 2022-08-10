package examples

import (
	"github.com/mit-pdos/go-mvcc/txn"
)

func WriteReservedKeySeq(txn *txn.Txn) bool {
	txn.Put(0, 2)
	return true
}

func WriteReservedKey(txn *txn.Txn) bool {
	return txn.DoTxn(WriteReservedKeySeq)
}

func WriteFreeKeySeq(txn *txn.Txn) bool {
	txn.Put(1, 3)
	return true
}

func WriteFreeKey(txn *txn.Txn) bool {
	return txn.DoTxn(WriteFreeKeySeq)
}

func WriteReservedKeyExample() (*uint64, bool) {
	mgr := txn.MkTxnMgr()
	p := new(uint64)
	mgr.InitializeData(p)
	txn := mgr.New()
	ok := WriteReservedKey(txn)
	if ok {
		*p = 2
	}
	return p, ok
}

func WriteFreeKeyExample() bool {
	mgr := txn.MkTxnMgr()
	p := new(uint64)
	mgr.InitializeData(p)
	txn := mgr.New()
	ok := WriteFreeKey(txn)
	return ok
}

