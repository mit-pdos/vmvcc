package examples

import (
	"github.com/mit-pdos/vmvcc/vmvcc"
)

func hello(txn *vmvcc.Txn) bool {
	txn.Write(0, "hello")
	txn.Read(0)
	txn.Delete(0)

	return true
}

func Hello(txno *vmvcc.Txn) {
	body := func(txni *vmvcc.Txn) bool {
		return hello(txni)
	}
	txno.Run(body)
}

func CallHello() {
	db := vmvcc.MkDB()
	db.ActivateGC()

	txn := db.NewTxn()
    Hello(txn)
}
