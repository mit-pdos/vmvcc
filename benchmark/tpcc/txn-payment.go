package tpcc

import (
	"fmt"
	"math/rand"
	"github.com/mit-pdos/go-mvcc/txn"
)

func payment(
	txn *txn.Txn,
	cid uint32, cdid uint8, cwid uint8, did uint8, wid uint8,
	hamount float32,
) bool {
	/* Read warehouse. */
	warehouse := NewWarehouse(wid)
	ReadTable(warehouse, txn)

	/* Update warehouse balance. */
	warehouse.UpdateBalance(hamount)
	WriteTable(warehouse, txn)

	/* Read district. */
	district := NewDistrict(did, wid)
	ReadTable(district, txn)

	/* Update district balance. */
	district.UpdateBalance(hamount)
	WriteTable(district, txn)

	/* Read customer. */
	customer := NewCustomer(cid, cdid, cwid)
	ReadTable(customer, txn)

	/* Update customer balance, payment, and payment count. */
	if customer.C_CREDIT == [2]byte{ 'B', 'C' } {
		/* Also update the data field if the customer has bad credit. */
		cdata := fmt.Sprintf("%d %d %d %d %d %.2f|%s",
			cid, cdid, cwid, did, wid, hamount, beforeNull(customer.C_DATA[:]))
		fmt.Printf("cdata = %s len(cdata) = %d\n", cdata, len(cdata))
		if len(cdata) > 500 {
			cdata = cdata[: 500]
		}
		customer.UpdateOnBadCredit(hamount, cdata)
	} else {
		customer.UpdateOnGoodCredit(hamount)
	}
	WriteTable(customer, txn)

	/* Randomly generate history record ID (not part of TPC-C). */
	exists := true
	var history *History
	for exists {
		hid := rand.Uint64()
		history = NewHistory(hid)
		exists = ReadTable(history, txn)
	}
	// TODO: Silo only uses increasing number for current time (tpcc.cc:328)
	var date uint32 = 0
	wname := beforeNull(warehouse.W_NAME[:])
	dname := beforeNull(district.D_NAME[:])
	hdata := fmt.Sprintf("%s    %s", wname, dname)
	history.Initialize(cid, cdid, cwid, did, wid, date, hamount, hdata)
	WriteTable(history, txn)

	return true
}

func TxnPayment(
	txno *txn.Txn,
	cid uint32, cdid uint8, cwid uint8, did uint8, wid uint8,
	hamount float32,
) bool {
	body := func(txni *txn.Txn) bool {
		return payment(txni, cid, cdid, cwid, did, wid, hamount)
	}
	ok := txno.DoTxn(body)
	return ok
}
