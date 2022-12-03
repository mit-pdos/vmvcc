package tpcc

import (
	"github.com/mit-pdos/go-mvcc/txn"
)

/**
 * Based on:
 * https://github.com/apavlo/py-tpcc/blob/7c3ff501bbe98a6a7abd3c13267523c3684b62d6/pytpcc/runtime/loader.py
 * TODO: Initialize other performance-unrelated fields according to TPC-C spec.
 */

func LoadTPCC(
	txn *txn.Txn,
	nItems uint32, nWarehouses uint8,
	nLocalDists uint8, nLocalCustomers uint32, nInitLocalNewOrders uint32,
) {
	for iid := uint32(1); iid <= nItems; iid++ {
		loadItem(txn, iid, true)
	}

	for wid := uint8(1); wid <= nWarehouses; wid++ {
		loadWarehouse(txn, wid)
		for did := uint8(1); did <= nLocalDists; did++ {
			loadDistrict(txn, did, wid, nInitLocalNewOrders + 1)

			for cid := uint32(1); cid <= nLocalCustomers; cid++ {
				loadCustomer(txn, cid, did, wid, true)
			}

			for oid := uint32(1); oid <= nInitLocalNewOrders; oid++ {
				loadNewOrder(txn, oid, did, wid)
			}
		}
	}
}

func loadItem(txn *txn.Txn, iid uint32, original bool) {
	// TODO: original data
	InsertItem(
		txn, iid,
		4, "item", 14.7, "data",
	)
}

func loadWarehouse(txn *txn.Txn, wid uint8) {
	InsertWarehouse(
		txn, wid,
		"name", "street1", "street2", "city",
		[2]byte{ 'M', 'A' }, [9]byte{ '0', '2', '1', '3', '9' },
		6.25, 80.0,
	)
}

func loadDistrict(txn *txn.Txn, did uint8, wid uint8, nextoid uint32) {
	InsertDistrict(
		txn, did, wid,
		"name", "street1", "street2", "city",
		[2]byte{ 'M', 'A' }, [9]byte{ '0', '2', '1', '3', '9' },
		6.25, 80.0, nextoid, 1,
	)
}

func loadCustomer(txn *txn.Txn, cid uint32, did uint8, wid uint8, bc bool) {
	InsertCustomer(
		txn,
		cid, did, wid,
		"first", [2]byte{'O', 'S'}, "last", "street1", "street2", "city",
		[2]byte{ 'M', 'A' }, [9]byte{ '0', '2', '1', '3', '9' },
		[16]byte{'0', '1'}, 1994, [2]byte{'B', 'C'}, 12.3, 43.1, 60.0, 80.0,
		3, 9, "data",
	)
}

func loadNewOrder(txn *txn.Txn, oid uint32, did uint8, wid uint8) {
	InsertNewOrder(txn, oid, did, wid)
}
