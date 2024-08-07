package main

import (
	// "fmt"
	"github.com/mit-pdos/vmvcc/vmvcc"
)

/**
 * Simplification:
 * 1. No select customer by last name.
 */

type OrderStatusOrderLineResult struct {
	OL_I_ID        uint32
	OL_SUPPLY_W_ID uint8
	OL_QUANTITY    uint8
	OL_AMOUNT      float32
	OL_DELIVERY_D  uint32
}

type OrderStatusResult struct {
	/* customer */
	C_BALANCE float32
	C_FIRST   [16]byte
	C_MIDDLE  [2]byte
	C_LAST    [16]byte
	/* order */
	O_ID         uint32
	O_ENTRY_D    uint32
	O_CARRIER_ID uint8
	/* order lines */
	OL_RES []OrderStatusOrderLineResult
}

func orderstatus(
	txn *vmvcc.Txn, ctx *TPCContext,
	/* input parameters */
	wid uint8, did uint8, cid uint32,
	/* return values */
	res *OrderStatusResult,
) bool {
	/* Read customer. */
	customer := &ctx.customer
	GetCustomerX(txn, cid, did, wid, customer)
	res.C_BALANCE = customer.C_BALANCE
	res.C_FIRST = customer.C_FIRST
	res.C_MIDDLE = customer.C_MIDDLE
	res.C_LAST = customer.C_LAST

	/* Get all orders of this customer. */
	orders := GetOrdersByIndex(txn, cid, did, wid)
	if len(orders) == 0 {
		return true
	}

	/* Pick the one with largest O_ID. */
	oidmax := orders[0].O_ID
	imax := 0
	for i := range orders {
		oid := orders[i].O_ID
		if oid > oidmax {
			oidmax = oid
			imax = i
		}
	}
	order := orders[imax]
	oid := order.O_ID
	res.O_ID = oid
	res.O_ENTRY_D = order.O_ENTRY_D
	res.O_CARRIER_ID = order.O_CARRIER_ID

	/* Get all order lines of that order. */
	olres := make([]OrderStatusOrderLineResult, 0, 10)
	for olnum := uint8(1); olnum <= 15; olnum++ {
		ol := &ctx.orderline
		found := GetOrderLineX(txn, ol, oid, did, wid, olnum)
		if !found {
			break
		}
		r := OrderStatusOrderLineResult{
			OL_I_ID:        ol.OL_I_ID,
			OL_SUPPLY_W_ID: ol.OL_SUPPLY_W_ID,
			OL_QUANTITY:    ol.OL_QUANTITY,
			OL_AMOUNT:      ol.OL_AMOUNT,
			OL_DELIVERY_D:  ol.OL_DELIVERY_D,
		}
		olres = append(olres, r)
	}
	res.OL_RES = olres

	return true
}

func TxnOrderStatus(txno *vmvcc.Txn, p *OrderStatusInput, ctx *TPCContext) (*OrderStatusResult, bool) {
	/* prepare output */
	res := new(OrderStatusResult)
	/* prepare input */
	wid := p.W_ID
	did := p.D_ID
	cid := p.C_ID
	body := func(txni *vmvcc.Txn) bool {
		return orderstatus(txni, ctx, wid, did, cid, res)
	}
	ok := txno.Run(body)
	return res, ok
}
