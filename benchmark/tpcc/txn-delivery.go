package main

import (
	// "fmt"
	"github.com/mit-pdos/vmvcc/vmvcc"
)

type DeliveryNewOrderResult struct {
	NO_D_ID uint8
	NO_O_ID uint32
}

type DeliveryResult struct {
	NO_RES []DeliveryNewOrderResult
}

func delivery(
	txn *vmvcc.Txn,
	wid uint8, did uint8, carrierid uint8, deliveryd uint32,
	res *DeliveryResult,
) bool {
	/* Find the oldest in-progress order. */
	district, _ := GetDistrict(txn, did, wid)
	oid := district.D_OLD_O_ID
	district.IncrementOldestOrderId(txn)

	_, found := GetNewOrder(txn, oid, did, wid)
	if !found {
		return true
	}

	/* Append to result. */
	noid := DeliveryNewOrderResult{NO_D_ID: did, NO_O_ID: oid}
	res.NO_RES = append(res.NO_RES, noid)

	/* Get the customer id of this order. */
	order, _ := GetOrder(txn, oid, did, wid)
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
	customer, _ := GetCustomer(txn, cid, did, wid)
	customer.IncreaseBalance(txn, total)

	return true
}

func TxnDelivery(txno *vmvcc.Txn, p *DeliveryInput) ([]*DeliveryResult, bool) {
	/* prepare output */
	ress := make([]*DeliveryResult, 10)
	/* prepare input */
	wid := p.W_ID
	ocarrierid := p.O_CARRIER_ID
	oldeliveryd := p.OL_DELIVERY_D
	for did := uint8(1); did <= 10; did++ {
		res := new(DeliveryResult)
		body := func(txni *vmvcc.Txn) bool {
			return delivery(txni, wid, did, ocarrierid, oldeliveryd, res)
		}
		/* Restart this transaction if fails. */
		for !txno.Run(body) {
		}
		ress[did-1] = res
	}
	return ress, true
}
