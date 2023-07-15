package examples

import (
	"github.com/mit-pdos/vmvcc/txn"
)

func hello(txn *txn.Txn) bool {
	txn.Put(0, "hello")
	txn.Get(0)
	txn.Delete(0)

	return true
}

func Hello(txno *txn.Txn) {
	body := func(txni *txn.Txn) bool {
		return hello(txni)
	}
	txno.DoTxn(body)
}

func CallHello() {
	db := txn.MkTxnMgr()
	db.ActivateGC()

	txn := db.New()
    Hello(txn)
}
