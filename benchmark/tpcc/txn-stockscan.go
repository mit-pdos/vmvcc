package main

import (
	// "fmt"
	"github.com/mit-pdos/vmvcc/vmvcc"
)

func stockscan(
	txn *vmvcc.Txn,
	/* input parameters */
	nwhs uint8, nitems uint32,
	/* return value */
	cnts []uint32,
) bool {
	/* Read all the stocks in this order. */
	for iid := uint32(1); iid <= nitems; iid++ {
		for wid := uint8(1); wid <= nwhs; wid++ {
			stock, _ := GetStock(txn, iid, wid)
			quantity := stock.S_QUANTITY
			cnts[iid - 1] += uint32(quantity)
		}
	}
	// fmt.Printf("Done stockscan.\n")

	return true
}

func TxnStockScan(txno *vmvcc.Txn, nwhs uint8, nitems uint32) ([]uint32, bool) {
	/* prepare output */
	cnts := make([]uint32, nitems)
	/* prepare input */
	body := func(txni *vmvcc.Txn) bool {
		return stockscan(txni, nwhs, nitems, cnts)
	}
	ok := txno.Run(body)
	return cnts, ok
}
