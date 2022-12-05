package tpcc

import (
	// "fmt"
	// "time"
	"math/rand"
	"github.com/mit-pdos/go-mvcc/txn"
)

/**
 * Based on:
 * https://github.com/apavlo/py-tpcc/blob/7c3ff501bbe98a6a7abd3c13267523c3684b62d6/pytpcc/runtime/loader.py
 * TODO: Initialize other performance-unrelated fields according to TPC-C spec.
 */

func panicf(ok bool) {
	if !ok {
		panic("txn.DoTxn should never fail here.")
	}
}

func LoadTPCCItems(txno *txn.Txn, nItems uint32) {
	for iid := uint32(1); iid <= nItems; iid++ {
		body := func(txni *txn.Txn) bool {
			loadItem(txni, iid, true)
			return true
		}
		panicf(txno.DoTxn(body))
	}
}

func LoadOneTPCCWarehouse(
	txno *txn.Txn, wid uint8,
	nItems uint32, nWarehouses uint8,
	nLocalDistricts uint8, nLocalCustomers uint32,
	nInitLocalNewOrders uint32,
) {
	rd := rand.New(rand.NewSource(int64(wid)))

	/* Compute the start of history id of each pair of warehouse and district */
	hid := uint64(wid - 1) * uint64(nLocalDistricts) * uint64(nLocalCustomers) + 1

	/* Load warehouse. */
	body := func(txni *txn.Txn) bool {
		loadWarehouse(txni, wid)
		return true
	}
	panicf(txno.DoTxn(body))

	for did := uint8(1); did <= nLocalDistricts; did++ {
		// fmt.Printf("Loading (W,D) = (%d,%d).\n", wid, did)

		/* Load district in each warehouse. */
		body = func(txni *txn.Txn) bool {
			loadDistrict(txni, did, wid, nInitLocalNewOrders + 1)
			return true
		}
		panicf(txno.DoTxn(body))

		/* Make a permutation of cids to be used for making Order. */
		cids := make([]uint32, nLocalCustomers)
		/* Load customer and history. */
		for cid := uint32(1); cid <= nLocalCustomers; cid++ {
			body = func(txni *txn.Txn) bool {
				/* Load customer in each pair of warehouse and district. */
				bc := rd.Uint32() % 10 < 1
				loadCustomer(txni, cid, did, wid, bc)
				loadHistory(txni, hid, cid, did, wid)
				hid++
				cids[cid - 1] = cid
				return true
			}
			panicf(txno.DoTxn(body))
		}

		/* Shuffle `cids`. */
		rd.Shuffle(len(cids), func(i, j int) {
			cids[i], cids[j] = cids[j], cids[i]
		})

		/* Every customer has one initial order. */
		for oid := uint32(1); oid <= nLocalCustomers; oid++ {
			body = func(txni *txn.Txn) bool {
				r := uint32(OL_MAX_CNT + 1 - OL_MIN_CNT)
				nOrderLines := uint8(rd.Uint32() % r + uint32(OL_MIN_CNT))
				isNewOrder := false
				if oid > nLocalCustomers - nInitLocalNewOrders {
					/* Load new order for the last `nInitLocalNewOrders`. */
					loadNewOrder(txni, oid, did, wid)
					isNewOrder = true
				}
				/* Load order in each pair of warehouse and district. */
				/* TODO: get current time */
				var entryd uint32 = 0
				loadOrder(
					txni, oid, did, wid, cids[oid - 1], entryd,
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
						txni, rd, oid, did, wid, olnum, entryd,
						nWarehouses, nItems, isNewOrder,
					)
				}
				return true
			}
			panicf(txno.DoTxn(body))
		}
	}
	for iid := uint32(1); iid <= nItems; iid++ {
		body = func(txni *txn.Txn) bool {
			/* Load stock. */
			loadStock(txni, iid, wid, false) // TODO: original
			return true
		}
		panicf(txno.DoTxn(body))
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
	txn *txn.Txn, rd *rand.Rand,
	oid uint32, did uint8, wid uint8, olnum uint8,
	entryd uint32, nwarehouses uint8, nitems uint32, isnew bool,
) {
	/* Randomly pick one item. */
	iid := pickBetween(rd, 1, nitems)

	/* ~1% of items are from remote warehouses. */
	supplywid := wid
	if trueWithProb(rd, 1) {
		supplywid = pickWarehouseIdExcept(rd, nwarehouses, wid)
	}

	var deliveryd uint32 = entryd
	var olamount float32 = 0.0
	if isnew {
		olamount = float32(rd.Uint32() % 999999 + 1) / 100
		deliveryd = OL_DELIVERY_D_NULL
	}

	InsertOrderLine(
		txn,
		oid, did, wid, olnum,
		iid, supplywid, deliveryd,
		OL_INIT_QUANTITY, olamount,
	)
}

func loadHistory(txn *txn.Txn, hid uint64, cid uint32, did uint8, wid uint8) {
	InsertHistory(
		txn,
		hid,
		cid, did, wid, did, wid, /* customer making orders to local district */
		12, H_INIT_AMOUNT, "",
	)
}

func loadStock(txn *txn.Txn, iid uint32, wid uint8, original bool) {
	var quantity uint16 = 20 // TODO
	var data string = "stockdata" // TODO: based on original
	InsertStock(
		txn,
		iid, wid,
		quantity, 0, 0, 0, data,
	)

}
