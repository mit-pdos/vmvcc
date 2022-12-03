package tpcc

import (
	"testing"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/mit-pdos/go-mvcc/txn"
)

/**
 * Tests for basic data layout/size and encoding primitives.
 */
func TestTableId(t *testing.T) {
	fmt.Printf("%.15x\n", TBLID_WAREHOUSE)
	fmt.Printf("%x\n", TBLID_DISTRICT)
	fmt.Printf("%x\n", TBLID_CUSTOMER)
	fmt.Printf("%x\n", TBLID_HISTORY)
	fmt.Printf("%x\n", TBLID_NEWORDER)
	fmt.Printf("%x\n", TBLID_ORDER)
	fmt.Printf("%x\n", TBLID_ORDERLINE)
	fmt.Printf("%x\n", TBLID_ITEM)
	fmt.Printf("%x\n", TBLID_STOCK)
	fmt.Printf("%x\n", IDXID_ORDER)
}

func TestGkey(t *testing.T) {
	warehouse := Warehouse { W_ID : 1 }
	fmt.Printf("%.15x\n", warehouse.gkey())
	district := District { D_ID : 1, D_W_ID : 1 }
	fmt.Printf("%x\n", district.gkey())
	customer := Customer { C_ID : 1, C_D_ID: 1, C_W_ID: 1 }
	fmt.Printf("%x\n", customer.gkey())
	history := History { H_ID : 1 }
	fmt.Printf("%x\n", history.gkey())
	neworder := NewOrder { NO_O_ID : 1, NO_D_ID : 1, NO_W_ID : 1 }
	fmt.Printf("%x\n", neworder.gkey())
	order := Order { O_ID : 1, O_D_ID : 1, O_W_ID : 1 }
	fmt.Printf("%x\n", order.gkey())
	orderline := OrderLine { OL_O_ID : 1, OL_D_ID : 1, OL_W_ID : 1, OL_NUMBER : 1 }
	fmt.Printf("%x\n", orderline.gkey())
	item := Item { I_ID : 1 }
	fmt.Printf("%x\n", item.gkey())
	stock := Stock { S_I_ID : 1, S_W_ID : 1 }
	fmt.Printf("%x\n", stock.gkey())
}

func TestRecordSize(t *testing.T) {
	var s string
	warehouse := Warehouse { W_ID : 1 }
	s = warehouse.encode()
	fmt.Printf("Warehouse record size = %d\n", len(s))
	district := District { D_ID : 1, D_W_ID : 1 }
	s = district.encode()
	fmt.Printf("District record size = %d\n", len(s))
	customer := Customer { C_ID : 1, C_D_ID: 1, C_W_ID: 1 }
	s = customer.encode()
	fmt.Printf("Customer record size = %d\n", len(s))
	history := History { H_ID : 1 }
	s = history.encode()
	fmt.Printf("History record size = %d\n", len(s))
	neworder := NewOrder { NO_O_ID : 1, NO_D_ID : 1, NO_W_ID : 1 }
	s = neworder.encode()
	fmt.Printf("NewOrder record size = %d\n", len(s))
	order := Order { O_ID : 1, O_D_ID : 1, O_W_ID : 1 }
	s = order.encode()
	fmt.Printf("Order record size = %d\n", len(s))
	orderline := OrderLine { OL_O_ID : 1, OL_D_ID : 1, OL_W_ID : 1, OL_NUMBER : 1 }
	s = orderline.encode()
	fmt.Printf("OrderLine record size = %d\n", len(s))
	item := Item { I_ID : 1 }
	s = item.encode()
	fmt.Printf("Item record size = %d\n", len(s))
	stock := Stock { S_I_ID : 1, S_W_ID : 1 }
	s = stock.encode()
	fmt.Printf("Stock record size = %d\n", len(s))
}

func TestEncodeDecodeCustomer(t *testing.T) {
	assert := assert.New(t)
	c := &Customer {
		C_ID : 14,
		C_W_ID : 223,
		C_LAST : [16]byte{4, 9},
	}
	s := c.encode()
	d := new(Customer)
	d.decode(s)
	assert.Equal(*c, *d)
}

func TestIndexEncodeDecode(t *testing.T) {
	assert := assert.New(t)
	gkeys := []uint64{ 4, 7, 2, 1, 81 }
	fmt.Printf("len(encodeidx(gkeys)) = %d\n", len(encodeidx(gkeys)))
	fmt.Printf("%v\n", decodeidx(encodeidx(gkeys)))
	assert.Equal(gkeys, decodeidx(encodeidx(gkeys)))
}


/**
 * Test for table operations.
 */
func TestTableWarehouse(t *testing.T) {
	assert := assert.New(t)
	mgr := txn.MkTxnMgr()
	txno := mgr.New()

	/* Insert a Warehouse record. */
	body := func(txn *txn.Txn) bool {
		InsertWarehouse(
			txn,
			41,
			"name", "street1", "street2", "city",
			[2]byte{'M', 'A'}, [9]byte{'0', '2', '1', '3', '9'},
			6.25, 80.0,
		)
		return true
	}
	ok := txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Read it, update it, and read it again in one transaction. */
	body = func(txn *txn.Txn) bool {
		x, found := GetWarehouse(txn, 41)
		assert.Equal(true, found)
		assert.Equal(uint8(41), x.W_ID)
		assert.Equal(float32(6.25), x.W_TAX)
		assert.Equal(float32(80.0), x.W_YTD)

		x.UpdateBalance(txn, 10.0)

		x, _ = GetWarehouse(txn, 41)
		assert.Equal(float32(90.0), x.W_YTD)
		return true
	}
	ok = txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Read it again. */
	body = func(txn *txn.Txn) bool {
		x, found := GetWarehouse(txn, 41)
		assert.Equal(true, found)
		assert.Equal(uint8(41), x.W_ID)
		assert.Equal(float32(6.25), x.W_TAX)
		assert.Equal(float32(90.0), x.W_YTD)
		return true
	}
	ok = txno.DoTxn(body)
	assert.Equal(true, ok)
}

func TestTableDistrict(t *testing.T) {
	assert := assert.New(t)
	mgr := txn.MkTxnMgr()
	txno := mgr.New()

	/* Insert a District record. */
	body := func(txn *txn.Txn) bool {
		InsertDistrict(
			txn,
			95, 41,
			"name", "street1", "street2", "city",
			[2]byte{'M', 'A'}, [9]byte{'0', '2', '1', '3', '9'},
			6.25, 80.0, 1, 1,
		)
		return true
	}
	ok := txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Read it, update it, and read it again in one transaction. */
	body = func(txn *txn.Txn) bool {
		x, found := GetDistrict(txn, 95, 41)
		assert.Equal(true, found)
		assert.Equal(uint8(95), x.D_ID)
		assert.Equal(uint8(41), x.D_W_ID)
		assert.Equal(float32(6.25), x.D_TAX)
		assert.Equal(float32(80.0), x.D_YTD)
		assert.Equal(uint32(1), x.D_NEXT_O_ID)
		assert.Equal(uint32(1), x.D_OLD_O_ID)

		x.IncrementNextOrderId(txn)
		x.IncrementOldestOrderId(txn)
		x.UpdateBalance(txn, 10.0)

		x, _ = GetDistrict(txn, 95, 41)
		assert.Equal(float32(90.0), x.D_YTD)
		assert.Equal(uint32(2), x.D_NEXT_O_ID)
		assert.Equal(uint32(2), x.D_OLD_O_ID)
		return true
	}
	ok = txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Read it again. */
	body = func(txn *txn.Txn) bool {
		x, found := GetDistrict(txn, 95, 41)
		assert.Equal(true, found)
		assert.Equal(float32(90.0), x.D_YTD)
		assert.Equal(uint32(2), x.D_NEXT_O_ID)
		assert.Equal(uint32(2), x.D_OLD_O_ID)
		return true
	}
	ok = txno.DoTxn(body)
	assert.Equal(true, ok)
}

func TestTableCustomer(t *testing.T) {
	assert := assert.New(t)
	mgr := txn.MkTxnMgr()
	txno := mgr.New()

	/* Insert a Customer record. */
	body := func(txn *txn.Txn) bool {
		InsertCustomer(
			txn,
			20, 95, 41,
			"first", [2]byte{'O', 'S'}, "last", "street1", "street2", "city",
			[2]byte{'M', 'A'}, [9]byte{'0', '2', '1', '3', '9'},
			[16]byte{'0', '1'}, 1994, [2]byte{'B', 'C'}, 12.3, 43.1, 60.0, 80.0,
			3, 9, "data",
		)
		return true
	}
	ok := txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Read it, update it, and read it again in one transaction. */
	body = func(txn *txn.Txn) bool {
		x, found := GetCustomer(txn, 20, 95, 41)
		assert.Equal(true, found)
		assert.Equal(uint32(20), x.C_ID)
		assert.Equal(uint8(95), x.C_D_ID)
		assert.Equal(uint8(41), x.C_W_ID)
		assert.Equal(float32(60.0), x.C_BALANCE)
		assert.Equal(float32(80.0), x.C_YTD_PAYMENT)
		assert.Equal(uint16(3), x.C_PAYMENT_CNT)
		assert.Equal("data", string(beforeNull(x.C_DATA[:])))

		x.UpdateOnBadCredit(txn, 10.0, "Hello Customer")

		x, _ = GetCustomer(txn, 20, 95, 41)
		assert.Equal(float32(50.0), x.C_BALANCE)
		assert.Equal(float32(90.0), x.C_YTD_PAYMENT)
		assert.Equal(uint16(4), x.C_PAYMENT_CNT)
		assert.Equal("Hello Customer", string(beforeNull(x.C_DATA[:])))
		return true
	}
	ok = txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Read it again. */
	body = func(txn *txn.Txn) bool {
		x, found := GetCustomer(txn, 20, 95, 41)
		assert.Equal(true, found)
		assert.Equal(uint32(20), x.C_ID)
		assert.Equal(uint8(95), x.C_D_ID)
		assert.Equal(uint8(41), x.C_W_ID)
		assert.Equal(float32(50.0), x.C_BALANCE)
		assert.Equal(float32(90.0), x.C_YTD_PAYMENT)
		assert.Equal(uint16(4), x.C_PAYMENT_CNT)
		assert.Equal("Hello Customer", string(beforeNull(x.C_DATA[:])))
		return true
	}
	ok = txno.DoTxn(body)
	assert.Equal(true, ok)
}


/**
 * Tests for TPC-C database loader.
 */
func TestLoader(t *testing.T) {
	assert := assert.New(t)
	mgr := txn.MkTxnMgr()
	txno := mgr.New()

	var ok bool
	// var nItems uint32 = N_ITEMS
	// var nWarehouses uint8 = 10
	// var nLocalDists uint8 = N_DISTRICTS_PER_WAREHOUSE
	// var nLocalCusts uint32 = N_CUSTOMERS_PER_DISTRICT
	// var nInitLocalNewOrders uint32 = N_INIT_NEW_ORDERS_PER_DISTRICT
	var nItems uint32 = 10
	var nWarehouses uint8 = 2
	var nLocalDists uint8 = 2
	var nLocalCusts uint32 = 10
	var nInitLocalNewOrders uint32 = 5
	var nInitLocalOrders = nLocalCusts
	assert.LessOrEqual(nInitLocalNewOrders, nInitLocalOrders)
	body := func(txni *txn.Txn) bool {
		LoadTPCC(
			txni, nItems, nWarehouses,
			nLocalDists, nLocalCusts, nInitLocalNewOrders, 
		)
		return true
	}
	ok = txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Testing items. */
	body = func(txni *txn.Txn) bool {
		var item *Item
		var found bool
		item, found = GetItem(txni, 0)
		assert.Equal(false, found)

		item, found = GetItem(txni, 1)
		assert.Equal(true, found)
		assert.Equal(uint32(1), item.I_ID)
		assert.Equal(float32(14.7), item.I_PRICE)

		item, found = GetItem(txni, nItems / 2)
		assert.Equal(true, found)
		assert.Equal(nItems / 2, item.I_ID)

		item, found = GetItem(txni, nItems)
		assert.Equal(true, found)
		assert.Equal(nItems, item.I_ID)

		item, found = GetItem(txni, nItems + 1)
		assert.Equal(false, found)

		/* TODO: Testing whether ~10% of items contain "ORIGINAL" in I_DATA. */
		return true
	}
	ok = txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Testing Warehouse. */
	body = func(txni *txn.Txn) bool {
		var warehouse *Warehouse
		var found bool
		for wid := uint8(0); wid <= nWarehouses + 1; wid++ {
			warehouse, found = GetWarehouse(txni, wid)
			if wid < 1 || wid > nWarehouses {
				assert.Equal(false, found)
			} else {
				assert.Equal(true, found)
				assert.Equal(wid, warehouse.W_ID)
			}
		}
		return true
	}
	ok = txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Testing District. */
	body = func(txni *txn.Txn) bool {
		var district *District
		var found bool
		for wid := uint8(0); wid <= nWarehouses + 1; wid++ {
			for did := uint8(0); did <= nLocalDists + 1; did++ {
				district, found = GetDistrict(txni, did, wid)
				if wid < 1 || wid > nWarehouses || did < 1 || did > nLocalDists {
					assert.Equal(false, found)
				} else {
					assert.Equal(true, found)
					assert.Equal(did, district.D_ID)
					assert.Equal(wid, district.D_W_ID)
				}
			}
		}
		return true
	}
	ok = txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Testing Customer. */
	body = func(txni *txn.Txn) bool {
		var customer *Customer
		var found bool
		for wid := uint8(0); wid <= nWarehouses + 1; wid++ {
			for did := uint8(0); did <= nLocalDists + 1; did++ {
				for cid := uint32(0); cid <= nLocalCusts + 1; cid++ {
					customer, found = GetCustomer(txni, cid, did, wid)
					if wid < 1 || wid > nWarehouses ||
						did < 1 || did > nLocalDists ||
						cid < 1 || cid > nLocalCusts {
						assert.Equal(false, found)
					} else {
						assert.Equal(true, found)
						assert.Equal(cid, customer.C_ID)
						assert.Equal(did, customer.C_D_ID)
						assert.Equal(wid, customer.C_W_ID)
					}
				}
			}
		}

		/* TODO: Testing whehther ~10% of customers have a bad credit. */
		return true
	}
	ok = txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Testing History. */
	body = func(txni *txn.Txn) bool {
		var history *History
		var found bool
		var nHistory uint64 = uint64(nWarehouses) * uint64(nLocalDists) * uint64(nLocalCusts)
		for hid := uint64(0); hid <= nHistory + 1; hid++ {
			history, found = GetHistory(txni, hid)
			if hid < 1 || hid > nHistory {
				assert.Equal(false, found)
			} else {
				assert.Equal(true, found)
				assert.Equal(hid, history.H_ID)
				assert.Less(uint8(0), history.H_W_ID)
				assert.LessOrEqual(history.H_W_ID, nWarehouses)
				assert.Less(uint8(0), history.H_D_ID)
				assert.LessOrEqual(history.H_D_ID, nLocalDists)
				assert.Less(uint32(0), history.H_C_ID)
				assert.LessOrEqual(history.H_C_ID, nLocalCusts)
			}
		}
		return true
	}
	ok = txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Testing Order, NewOrder, and OrderLine. */
	body = func(txni *txn.Txn) bool {
		/* For testing distribution. */
		var cntTotalItems uint64 = 0
		var cntRemoteItems uint64 = 0

		var order *Order
		var neworder *NewOrder
		var orderline *OrderLine
		var found bool
		for wid := uint8(0); wid <= nWarehouses + 1; wid++ {
			for did := uint8(0); did <= nLocalDists + 1; did++ {
				for oid := uint32(0); oid <= nInitLocalOrders + 1; oid++ {
					/* Order. */
					order, found = GetOrder(txni, oid, did, wid)
					if wid < 1 || wid > nWarehouses ||
						did < 1 || did > nLocalDists ||
						oid < 1 || oid > nInitLocalOrders {
						assert.Equal(false, found)
					} else {
						assert.Equal(true, found)
						assert.Equal(oid, order.O_ID)
						assert.Equal(did, order.O_D_ID)
						assert.Equal(wid, order.O_W_ID)
						assert.LessOrEqual(uint32(1), order.O_C_ID)
						assert.LessOrEqual(order.O_C_ID, nLocalCusts)
						assert.LessOrEqual(MIN_INIT_OL_CNT, order.O_OL_CNT)
						assert.LessOrEqual(order.O_OL_CNT, MAX_INIT_OL_CNT)
					}

					/* NewOrder, (newoidlb, newoidub] are new orders. */
					newoidlb := nInitLocalOrders - nInitLocalNewOrders
					newoidub := nInitLocalOrders
					neworder, found = GetNewOrder(txni, oid, did, wid)
					if wid < 1 || wid > nWarehouses ||
						did < 1 || did > nLocalDists ||
						oid <= newoidlb || oid > newoidub {
						assert.Equal(false, found)
					} else {
						assert.Equal(true, found)
						assert.Equal(oid, neworder.NO_O_ID)
						assert.Equal(did, neworder.NO_D_ID)
						assert.Equal(wid, neworder.NO_W_ID)
					}

					/* Orderline. */
					olcnt := order.O_OL_CNT
					for olnum := uint8(1); olnum <= olcnt + 1; olnum++ {
						orderline, found = GetOrderLine(txni, oid, did, wid, olnum)
						if wid < 1 || wid > nWarehouses ||
							did < 1 || did > nLocalDists ||
							oid < 1 || oid > nInitLocalOrders ||
							olnum < 1 || olnum > olcnt {
							assert.Equal(false, found)
						} else {
							assert.Equal(true, found)
							assert.Equal(oid, orderline.OL_O_ID)
							assert.Equal(did, orderline.OL_D_ID)
							assert.Equal(wid, orderline.OL_W_ID)
							assert.Equal(olnum, orderline.OL_NUMBER)
							olamount := orderline.OL_AMOUNT
							if orderline.OL_DELIVERY_D == OL_DELIVERY_D_NULL {
								/* This is a new order. */
								assert.LessOrEqual(float32(0.01), olamount)
								assert.LessOrEqual(olamount, float32(9999.99))
							} else {
								assert.Equal(float32(0.0), olamount)
							}
							cntTotalItems++
							if orderline.OL_W_ID != orderline.OL_SUPPLY_W_ID {
								cntRemoteItems++
							}
						}
					}
				}
			}
		}
		/* Check that the remote orderline is about ~1%. */
		ratioRemoteItems := float64(cntRemoteItems) / float64(cntTotalItems) * 100.0
		fmt.Printf("Ratio of remote items = %f%% (sholud be ~1%%).\n", ratioRemoteItems)
		return true
	}
	ok = txno.DoTxn(body)
	assert.Equal(true, ok)
}


/**
 * Tests for "business" transactions.
 */
func TestPayment(t *testing.T) {
	assert := assert.New(t)
	mgr := txn.MkTxnMgr()
	txno := mgr.New()

	// TODO: randomly generating below according to TPC-C spec
	cid  := uint32(1)
	cdid := uint8(2)
	cwid := uint8(3)
	did  := uint8(4)
	wid  := uint8(5)
	hamount := float32(10.0)

	/* Insert a Customer record. */
	var ok bool
	body := func(txn *txn.Txn) bool {
		InsertCustomer(
			txn,
			20, 95, 41,
			"first", [2]byte{'O', 'S'}, "last", "street1", "street2", "city",
			[2]byte{'M', 'A'}, [9]byte{'0', '2', '1', '3', '9'},
			[16]byte{'0', '1'}, 1994, [2]byte{'B', 'C'}, 12.3, 43.1, 60.0, 80.0,
			3, 9, "data",
		)
		return true
	}
	ok = txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Run Payment transaction twice. */
	ok = TxnPayment(txno, wid, did, hamount, cwid, cdid, cid)
	assert.Equal(true, ok)
	ok = TxnPayment(txno, wid, did, hamount, cwid, cdid, cid)
	assert.Equal(true, ok)
}
