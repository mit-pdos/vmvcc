package tpcc

import (
	"github.com/mit-pdos/go-mvcc/txn"
)

/**
 * Based on:
 * https://github.com/apavlo/py-tpcc/blob/7c3ff501bbe98a6a7abd3c13267523c3684b62d6/pytpcc/runtime/loader.py
 */

func LoadTPCC(txn *txn.Txn, nwarehouse uint8) {
	loadItem(txn, N_ITEMS)
}

func loadItem(txn *txn.Txn, nitem uint32) {
	for iid := uint32(1); iid <= nitem; iid++ {
		InsertItem(
			txn, iid,
			4, "item", 14.7, "data",
		)
	}
}
