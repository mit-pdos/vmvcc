package tpcc

import (
	// "fmt"
	"bytes"
	"github.com/mit-pdos/go-mvcc/txn"
)

type ItemInfo struct {
	I_NAME        [24]byte
	S_QUANTITY    uint16
	BRAND_GENERIC byte
	I_PRICE       float32
	OL_AMOUNT     float32
}

type NewOrderResult struct {
	W_TAX       float32
	D_TAX       float32
	D_NEXT_O_ID uint32
	TOTAL       float32
}

func neworder(
	txn *txn.Txn,
	/* input parameter */
	wid uint8, did uint8, cid uint32, oentryd uint32,
	iids []uint32, iwids []uint8, iqtys []uint8,
	/* return value */
	cret *Customer, res *NewOrderResult, iinfos *[]ItemInfo,
) bool {
	/* Determine whether this is a local transaction. */
	alllocal := true
	for _, iwid := range iwids {
		if iwid != wid {
			alllocal = false
			break
		}
	}

	var olcnt uint8 = uint8(len(iids))
	var ocarrierid uint8 = O_CARRIER_ID_NULL

	/* For each item, read their info. */
	items := make([]*Item, 0, len(iids))
	for _, iid := range iids {
		item, found := GetItem(txn, iid)
		/* Abort if one of the iids is invalid. (1% as specified by TPC-C.) */
		if !found {
			return false
		}
		items = append(items, item)
	}

	/* Read warehouse. */
	warehouse, _ := GetWarehouse(txn, wid)
	res.W_TAX = warehouse.W_TAX

	/* Read district. */
	district, _ := GetDistrict(txn, did, wid)
	res.D_TAX = district.D_TAX
	res.D_NEXT_O_ID = district.D_NEXT_O_ID
	oid := district.D_NEXT_O_ID

	/* Read customer. */
	customer, _ := GetCustomer(txn, cid, did, wid)
	*cret = *customer

	/* Increment next order id of district. */
	district.IncrementNextOrderId(txn)

	/* Insert an Order record. */
	InsertOrder(
		txn, oid, did, wid,
		cid, oentryd, ocarrierid, olcnt, alllocal,
	)

	/* Insert a NewOrder record. */
	InsertNewOrder(txn, oid, did, wid)

	/* For each item, read and update stock, create an order line. */
	var total float32 = 0
	for i, iid := range iids {
		/* Read stock. */
		iwid := iwids[i]
		stock, found := GetStock(txn, iid, iwid)
		if !found {
			continue
		}

		/* Retrieve item values to be used later. */
		iname  := items[i].I_NAME
		iprice := items[i].I_PRICE
		idata  := items[i].I_DATA

		/* Retrieve current stock values. */
		squantity  := stock.S_QUANTITY
		sytd       := stock.S_YTD
		sordercnt  := stock.S_ORDER_CNT
		sremotecnt := stock.S_REMOTE_CNT
		sdist      := stock.S_DISTS[did]
		sdata      := stock.S_DATA

		/* Compute new stock values. */
		olquantity := iqtys[i]
		if squantity < uint16(olquantity + 10) {
			squantity += 91
		}
		squantity -= uint16(olquantity)
		sytd += uint32(olquantity)
		sordercnt += 1
		if iwid != wid {
			sremotecnt += 1
		}

		/* Write stock. */
		stock.Update(txn, squantity, sytd, sordercnt, sremotecnt)

		/* Compute some return values. */
		var brandgeneric byte = 'G'
		og := []byte("ORIGINAL")
		if bytes.Contains(sdata[:], og) && bytes.Contains(idata[:], og) {
			brandgeneric = 'B'
		}
		olamount := float32(olquantity) * iprice
		total += olamount

		/* Insert an OrderLine record. */
		olnum := uint8(i) + 1
		InsertOrderLine(
			txn, oid, did, wid, olnum,
			iid, iwid, oentryd, olquantity, olamount, sdist,
		)

		/* TODO: Collect other return values. */
		iinfo := ItemInfo {
			I_NAME        : iname,
			S_QUANTITY    : squantity,
			BRAND_GENERIC : brandgeneric,
			I_PRICE       : iprice,
			OL_AMOUNT     : olamount,
		}
		*iinfos = append(*iinfos, iinfo)
	}
	res.TOTAL = total

	return true
}

func TxnNewOrder(txno *txn.Txn, p *NewOrderInput) (*Customer, *NewOrderResult, []ItemInfo, bool) {
	/* prepare output */
	customer := new(Customer)
	res := new(NewOrderResult)
	iinfos := make([]ItemInfo, 0)
	/* prepare input */
	wid := p.W_ID
	did := p.D_ID
	cid := p.C_ID
	oentryd := p.O_ENTRY_D
	iids := p.I_IDS
	iwids:= p.I_W_IDS
	iqtys := p.I_QTYS
	body := func(txni *txn.Txn) bool {
		return neworder(
			txni, wid, did, cid, oentryd, iids, iwids, iqtys, 
			customer, res, &iinfos,
		)
	}
	ok := txno.DoTxn(body)
	return customer, res, iinfos, ok
}
