package tpcc

import (
	"fmt"
	"math/rand"
	"github.com/mit-pdos/go-mvcc/txn"
)

/**
 * Based on:
 * https://github.com/apavlo/py-tpcc/blob/7c3ff501bbe98a6a7abd3c13267523c3684b62d6/pytpcc/runtime/loader.py
 * TODO: Initialize other performance-unrelated fields according to TPC-C spec.
 */

func LoadTPCCItems(txn *txn.Txn, nItems uint32) {
	for iid := uint32(1); iid <= nItems; iid++ {
		loadItem(txn, iid, true)
	}
}

func LoadOneTPCCWarehouse(
	txn *txn.Txn, wid uint8,
	nItems uint32, nWarehouses uint8,
	nLocalDistricts uint8, nLocalCustomers uint32,
	nInitLocalNewOrders uint32,
) {
	hid := uint64(wid - 1) * uint64(nLocalDistricts) * uint64(nLocalCustomers) + 1

	/* Load warehouse. */
	loadWarehouse(txn, wid)
	for did := uint8(1); did <= nLocalDistricts; did++ {
		fmt.Printf("Loading (W,D) = (%d,%d).\n", wid, did)
		/* Load district in each warehouse. */
		loadDistrict(txn, did, wid, nInitLocalNewOrders + 1)

		/* Make a permutation of cids to be used for making Order. */
		cids := make([]uint32, nLocalCustomers)
		for cid := uint32(1); cid <= nLocalCustomers; cid++ {
			/* Load customer in each pair of warehouse and district. */
			bc := rand.Uint32() % 10 < 1
			loadCustomer(txn, cid, did, wid, bc)
			loadHistory(txn, hid, cid, did, wid)
			hid++
			cids[cid - 1] = cid
		}

		/* Shuffle `cids`. */
		rand.Shuffle(len(cids), func(i, j int) {
			cids[i], cids[j] = cids[j], cids[i]
		})

		/* Every customer has one initial order. */
		for oid := uint32(1); oid <= nLocalCustomers; oid++ {
			r := uint32(MAX_INIT_OL_CNT + 1 - MIN_INIT_OL_CNT)
			nOrderLines := uint8(rand.Uint32() % r + uint32(MIN_INIT_OL_CNT))
			isNewOrder := false
			if oid > nLocalCustomers - nInitLocalNewOrders {
				/* Load new order for the last `nInitLocalNewOrders`. */
				loadNewOrder(txn, oid, did, wid)
				isNewOrder = true
			}
			/* Load order in each pair of warehouse and district. */
			/* TODO: get current time */
			var entryd uint32 = 0
			loadOrder(
				txn, oid, did, wid, cids[oid - 1], entryd,
				nOrderLines, isNewOrder,
			)

			/**
			 * Load order line.
			 * The reference implementation starts from 0, but I think it
			 * should start from 1.
			 * See our txn-neworder.go:L129, or their drivers/sqlitedriver.py:L276.
			 */
			for olnum := uint8(1); olnum <= nOrderLines; olnum++ {
				/* Load order line in each order. */
				loadOrderLine(
					txn, oid, did, wid, olnum, entryd,
					nWarehouses, nItems, isNewOrder,
				)
			}
		}
	}
	for iid := uint32(1); iid <= nItems; iid++ {
		/* Load stock. */
		loadStock(txn, iid, wid, false) // TODO: original
	}
}


/**
 * Sequential loader, could be very slow. Exists for demo/testing.
 */
func LoadTPCCSeq(
	txn *txn.Txn,
	nItems uint32, nWarehouses uint8,
	nLocalDistricts uint8, nLocalCustomers uint32, nInitLocalNewOrders uint32,
) {
	LoadTPCCItems(txn, nItems)

	for wid := uint8(1); wid <= nWarehouses; wid++ {
		LoadOneTPCCWarehouse(
			txn, wid, nItems, nWarehouses,
			nLocalDistricts, nLocalCustomers,
			nInitLocalNewOrders,
		)
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
	var credit [2]byte
	if bc {
		credit = [2]byte{ 'B', 'C' }
	} else {
		credit = [2]byte{ 'G', 'C' }
	}
	InsertCustomer(
		txn,
		cid, did, wid,
		"first", [2]byte{'O', 'S'}, "last", "street1", "street2", "city",
		[2]byte{ 'M', 'A' }, [9]byte{ '0', '2', '1', '3', '9' },
		[16]byte{ '0', '1' }, 1994, credit, 12.3, 43.1, 60.0, 80.0,
		3, 9, "data",
	)
}

func loadOrder(
	txn *txn.Txn, oid uint32, did uint8, wid uint8,
	cid uint32, entryd uint32, olcnt uint8, isnew bool,
) {
	InsertOrder(
		txn,
		oid, did, wid,
		cid, entryd, 4 /* TODO: O_CARRIER_D */,
		olcnt, true,
	)
}

func loadNewOrder(txn *txn.Txn, oid uint32, did uint8, wid uint8) {
	InsertNewOrder(txn, oid, did, wid)
}

func loadOrderLine(
	txn *txn.Txn, oid uint32, did uint8, wid uint8, olnum uint8,
	entryd uint32, nwarehouses uint8, nitems uint32, isnew bool,
) {
	/* Randomly pick one item. */
	iid := rand.Uint32() % nitems + 1
	supplywid := wid

	/* ~1% of items are from remote warehouses. */
	if rand.Intn(100) < 1 {
		for supplywid == wid {
			supplywid = uint8(rand.Uint32() % uint32(nwarehouses))
		}
	}

	var deliveryd uint32 = entryd
	var olamount float32 = 0.0
	if isnew {
		olamount = float32(rand.Uint32() % 999999 + 1) / 100
		deliveryd = OL_DELIVERY_D_NULL
	}

	InsertOrderLine(
		txn,
		oid, did, wid, olnum,
		iid, supplywid, deliveryd,
		ORDERLINE_INIT_QUANTITY, olamount,
		[24]byte{} /* TODO: OL_DIST_INFO */,
	)
}

func loadHistory(txn *txn.Txn, hid uint64, cid uint32, did uint8, wid uint8) {
	InsertHistory(
		txn,
		hid,
		cid, did, wid, did, wid, /* customer making orders to local district */
		12, HISTORY_INIT_AMOUNT, "",
	)
}

func loadStock(txn *txn.Txn, iid uint32, wid uint8, original bool) {
	var quantity uint16 = 20 // TODO
	var dists [10][24]byte // TODO
	var data string = "stockdata" // TODO: based on original
	InsertStock(
		txn,
		iid, wid,
		quantity, dists, 0, 0, 0, data,
	)

}
