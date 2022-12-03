package tpcc

import (
	// "fmt"
	"github.com/mit-pdos/go-mvcc/txn"
)

type DeliveryNewOrderResult struct {
	NO_D_ID uint8
	NO_O_ID uint32
}

type DeliveryResult struct {
	NO_RES []DeliveryNewOrderResult
}

func delivery(
	txn *txn.Txn,
	wid uint8, carrierid uint8, deliveryd uint32,
	res *DeliveryResult,
) bool {
	/* For each district, find the oldest in-progress order. */
	for did := uint8(1); did <= 10; did ++ {
		district := GetDistrict(txn, did, wid)
		oid := district.D_OLD_O_ID
		district.IncrementOldestOrderId(txn)

		_, found := GetNewOrder(txn, oid, did, wid)
		if !found {
			continue
		}

		/* Append to result. */
		noid := DeliveryNewOrderResult { NO_D_ID : did, NO_O_ID : oid }
		res.NO_RES = append(res.NO_RES, noid)

		/* Get the customer id of this order. */
		order := GetOrder(txn, oid, did, wid)
		cid := order.O_C_ID

		/* Update the carrier id of this order. */
		order.UpdateCarrier(txn, carrierid)

		/* Sum the total of this order. */
		var total float32 = 0
		for olnum := uint8(1); olnum <= 15; olnum++ {
			/* Get the order line. */
			ol, found := GetOrderLine(txn, oid, did, wid, olnum)
			if !found {
				break
			}
			/* Update the delivery date of each order line. */
			ol.UpdateDeliveryDate(txn, deliveryd)
			total += ol.OL_AMOUNT
		}

		/* Delete this order from NewOrder. */
		DeleteNewOrder(txn, oid, did, wid)

		/* Update the customer with  */
		customer := GetCustomer(txn, cid, did, wid)
		customer.IncreaseBalance(txn, total)
	}

	return true
}

func TxnDelivery(
	txno *txn.Txn,
	wid uint8, ocarrierid uint8, oldeliveryd uint32,
) (*DeliveryResult, bool) {
	res := new(DeliveryResult)
	body := func(txni *txn.Txn) bool {
		return delivery(txni, wid, ocarrierid, oldeliveryd, res)
	}
	ok := txno.DoTxn(body)
	return res, ok
}