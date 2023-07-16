package examples

import (
	"github.com/mit-pdos/vmvcc/txn"
)

func hello(txn *txn.Txn) bool {
	txn.Write(0, "hello")
	txn.Read(0)
	txn.Delete(0)

	return true
}

func Hello(txno *txn.Txn) {
	body := func(txni *txn.Txn) bool {
		return hello(txni)
	}
	txno.Run(body)
}

func CallHello() {
	db := txn.MkTxnMgr()
	db.ActivateGC()

	txn := db.New()
    Hello(txn)
}
