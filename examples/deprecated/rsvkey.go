package examples

import (
	"github.com/mit-pdos/vmvcc/txn"
)

func WriteReservedKeySeq(txn *txn.Txn, v uint64) bool {
	txn.Write(0, v)
	return true
}

func WriteReservedKey(t *txn.Txn, v uint64) bool {
	body := func(txn *txn.Txn) bool {
		return WriteReservedKeySeq(txn, v)
	}
	return t.Run(body)
}

func WriteFreeKeySeq(txn *txn.Txn, v uint64) bool {
	txn.Write(1, v)
	return true
}

func WriteFreeKey(t *txn.Txn, v uint64) bool {
	body := func(txn *txn.Txn) bool {
		return WriteFreeKeySeq(txn, v)
	}
	return t.Run(body)
}

func InitializeData(mgr *txn.TxnMgr) {
}

func InitExample() *txn.TxnMgr {
	mgr := txn.MkTxnMgr()
	InitializeData(mgr)
	return mgr
}

func WriteReservedKeyExample(mgr *txn.TxnMgr, v uint64) bool {
	txn := mgr.New()
	ok := WriteReservedKey(txn, v)
	return ok
}

func WriteFreeKeyExample(mgr *txn.TxnMgr, v uint64) bool {
	txn := mgr.New()
	ok := WriteFreeKey(txn, v)
	return ok
}
