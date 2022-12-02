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

func neworder(
	txn *txn.Txn,
	/* input parameter */
	wid uint8, did uint8, cid uint32, oentryd uint32,
	iids []uint32, iwids []uint8, iqtys []uint8,
	/* return value */
	iinfos *[]ItemInfo,
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
		item := NewItem(iid)
		found := ReadTable(item, txn)
		/* Abort if one of the iids is invalid. (1% as specified by TPC-C.) */
		if !found {
			return false
		}
		items = append(items, item)
	}

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

	/* Insert an Order record. */
	order := NewOrderRecord(oid, did, wid)
	order.Initialize(cid, oentryd, ocarrierid, olcnt, alllocal)
	WriteTable(order, txn)

	/* Insert a NewOrder record. */
	neworder := NewNewOrder(oid, did, wid)
	WriteTable(neworder, txn)

	/* For each item, read and update stock, create an order line. */
	for i, iid := range iids {
		/* Read stock. */
		iwid := iwids[i]
		stock := NewStock(iid, iwid)
		found := ReadTable(stock, txn)
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
		stock.Update(squantity, sytd, sordercnt, sremotecnt)
		WriteTable(stock, txn)

		/* Compute some return values. */
		var brandgeneric byte = 'G'
		og := []byte("ORIGINAL")
		if bytes.Contains(sdata[:], og) && bytes.Contains(idata[:], og) {
			brandgeneric = 'B'
		}
		olamount := float32(olquantity) * iprice

		/* Insert an OrderLine record. */
		olnum := uint8(i) + 1
		orderline := NewOrderLine(oid, did, wid, olnum)
		orderline.Initialize(iid, iwid, oentryd, olquantity, olamount, sdist)
		WriteTable(orderline, txn)

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

	return true
}

func TxnNewOrder(
	txno *txn.Txn,
	wid uint8, did uint8, cid uint32, oentryd uint32,
	iids []uint32, iwids []uint8, iqtys []uint8,
) ([]ItemInfo, bool) {
	iinfos := make([]ItemInfo, 0)
	body := func(txni *txn.Txn) bool {
		return neworder(txni, wid, did, cid, oentryd, iids, iwids, iqtys, &iinfos)
	}
	ok := txno.DoTxn(body)
	return iinfos, ok
}
