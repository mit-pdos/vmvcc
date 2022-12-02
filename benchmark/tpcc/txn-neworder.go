package tpcc

import (
	// "fmt"
	"github.com/mit-pdos/go-mvcc/txn"
)

func neworder(
	txn *txn.Txn,
	wid uint8, did uint8, cid uint32, oentryd uint32,
	iids []uint32, iwids []uint8, iqtys []uint8,
) bool {
	/* Determine whether this is a local transaction. */
	alllocal := true
	for _, iwid := range iwids {
		if iwid != wid {
			alllocal = false
			break
		}
	}

	/* For each item, read their info. */
	// for i, iid := range iids {
	// }
	var olcnt uint8 = 0
	var ocarrierid uint8 = 0

	/* If not every item id is valid, abort. (1% as specified by TPC-C.) */

	/* Read warehouse. */
	warehouse := NewWarehouse(wid)
	ReadTable(warehouse, txn)

	/* Read district. */
	district := NewDistrict(did, wid)
	ReadTable(district, txn)
	oid := district.D_NEXT_O_ID

	/* Read customer. */
	customer := NewCustomer(cid, did, wid)
	ReadTable(customer, txn)

	/* Increment next order id of district. */
	district.IncrementNextOrderId()
	WriteTable(district, txn)

	/* Insert a order. */
	order := NewOrderRecord(oid, did, wid)
	// TODO: ocarrierid, olcnt, alllocal
	order.Initialize(cid, oentryd, ocarrierid, olcnt, alllocal)
	WriteTable(order, txn)

	/* Insert a new order. */
	neworder := NewNewOrder(oid, did, wid)
	WriteTable(neworder, txn)

	/* For each item, read and update stock, create an order line. */

	return true
}

func TxnNewOrder(
	txno *txn.Txn,
	wid uint8, did uint8, cid uint32, oentryd uint32,
	iids []uint32, iwids []uint8, iqtys []uint8,
) bool {
	body := func(txni *txn.Txn) bool {
		return neworder(txni, wid, did, cid, oentryd, iids, iwids, iqtys)
	}
	ok := txno.DoTxn(body)
	return ok
}
