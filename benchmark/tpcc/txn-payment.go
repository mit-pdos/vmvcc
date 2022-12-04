package tpcc

import (
	"fmt"
	"math/rand"
	"github.com/mit-pdos/go-mvcc/txn"
)

func payment(
	txn *txn.Txn,
	wid uint8, did uint8, hamount float32, cwid uint8, cdid uint8, cid uint32,
) bool {
	/* Read warehouse. */
	warehouse, _ := GetWarehouse(txn, wid)

	/* Update warehouse balance. */
	warehouse.UpdateBalance(txn, hamount)

	/* Read district. */
	district, _ := GetDistrict(txn, did, wid)

	/* Update district balance. */
	district.UpdateBalance(txn, hamount)

	/* Read customer. */
	customer, _ := GetCustomer(txn, cid, cdid, cwid)

	/* Update customer balance, payment, and payment count. */
	if customer.C_CREDIT == [2]byte{ 'B', 'C' } {
		/* Also update the data field if the customer has bad credit. */
		cdata := fmt.Sprintf("%d %d %d %d %d %.2f|%s",
			cid, cdid, cwid, did, wid, hamount, beforeNull(customer.C_DATA[:]))
		// fmt.Printf("cdata = %s len(cdata) = %d\n", cdata, len(cdata))
		if len(cdata) > 500 {
			cdata = cdata[: 500]
		}
		customer.UpdateOnBadCredit(txn, hamount, cdata)
	} else {
		customer.UpdateOnGoodCredit(txn, hamount)
	}

	/* Randomly generate history record id (not part of TPC-C). */
	exists := true
	var hid uint64
	for exists {
		hid = rand.Uint64() % (1 << 56) /* MSB reserved for table ID */
		_, exists = GetHistory(txn, hid)
		if exists {
			fmt.Printf("H_ID collides, regenerate a new one.")
		}
	}

	/* Insert a history record. */
	// TODO: Silo only uses increasing number for current time (tpcc.cc:328)
	var date uint32 = 0
	wname := beforeNull(warehouse.W_NAME[:])
	dname := beforeNull(district.D_NAME[:])
	hdata := fmt.Sprintf("%s    %s", wname, dname)
	InsertHistory(txn, hid, cid, cdid, cwid, did, wid, date, hamount, hdata)

	return true
}

func TxnPayment(txno *txn.Txn, p *PaymentInput) bool {
	wid := p.W_ID
	did := p.D_ID
	hamount := p.H_AMOUNT
	cwid := p.C_W_ID
	cdid := p.C_D_ID
	cid := p.C_ID

	body := func(txni *txn.Txn) bool {
		return payment(txni, wid, did, hamount, cwid, cdid, cid)
	}
	ok := txno.DoTxn(body)
	return ok
}
