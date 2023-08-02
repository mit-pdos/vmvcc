package examples

import (
	"github.com/mit-pdos/vmvcc/examples/strnum"
	"github.com/mit-pdos/vmvcc/vmvcc"
)

func xfer(txn *vmvcc.Txn, src, dst, amt uint64) bool {
	sbalx, _ := txn.Read(src)
	sbal := strnum.StringToU64(sbalx)
	
	if sbal < amt {
		return false
	}

	sbaly := strnum.U64ToString(sbal - amt)
	txn.Write(src, sbaly)

	dbalx, _ := txn.Read(dst)
	dbal := strnum.StringToU64(dbalx)

	if dbal + amt < dbal {
		return false
	}

	dbaly := strnum.U64ToString(dbal + amt)
	txn.Write(dst, dbaly)

	return true
}

func AtomicXfer(txno *vmvcc.Txn, src, dst, amt uint64) bool {
	body := func(txni *vmvcc.Txn) bool {
		return xfer(txni, src, dst, amt)
	}
	return txno.Run(body)
}
