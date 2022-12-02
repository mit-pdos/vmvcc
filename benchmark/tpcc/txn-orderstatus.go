package tpcc

import (
	// "fmt"
	"github.com/mit-pdos/go-mvcc/txn"
)

/**
 * Simplification:
 * 1. No select customer by last name.
 */

func orderstatus(
	txn *txn.Txn,
	/* input parameters */
	wid uint8, did uint8, cid uint32,
	/* return values TODO */
	cret **Customer,
) bool {
	/* Read customer. */
	customer := NewCustomer(cid, did, wid)
	ReadTable(customer, txn)

	/* TODO: Get last order. */

	/* TODO: Get all order lines of that order. */

	return true
}

func TxnOrderStatus(
	txno *txn.Txn,
	wid uint8, did uint8, cid uint32,
) (*Customer, bool) {
	customer := new(Customer)
	body := func(txni *txn.Txn) bool {
		return orderstatus(txni, wid, did, cid, &customer)
	}
	ok := txno.DoTxn(body)
	return customer, ok
}
