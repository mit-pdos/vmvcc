package tpcc

import (
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
	customer := NewCustomer(cid, did, wid)
	ReadTable(customer, txn)

	/* Based on the  */
	credit := customer.C_CREDIT
	if credit == [2]byte{ 'B', 'C' } {
		customer.UpdateBadCredit(cid, cdid, cwid, did, wid, hamount)
	} else {
		// TODO: UpdateGoodCredit
	}

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
