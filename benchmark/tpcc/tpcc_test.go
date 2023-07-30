package main

import (
	"testing"
	"fmt"
	"bytes"
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"github.com/mit-pdos/vmvcc/vmvcc"
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

func encodeSlow(x any) uint64 {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, x)
	return uint64(len(buf.String()))
}

func TestRecordSize(t *testing.T) {
	assert := assert.New(t)
	warehouse := Warehouse { W_ID : 1 }
	assert.Equal(encodeSlow(warehouse), X_W_LEN)
	district := District { D_ID : 1, D_W_ID : 1 }
	assert.Equal(encodeSlow(district), X_D_LEN)
	customer := Customer { C_ID : 1, C_D_ID: 1, C_W_ID: 1 }
	assert.Equal(encodeSlow(customer), X_C_LEN)
	history := History { H_ID : 1 }
	assert.Equal(encodeSlow(history), X_H_LEN)
	neworder := NewOrder { NO_O_ID : 1, NO_D_ID : 1, NO_W_ID : 1 }
	assert.Equal(encodeSlow(neworder), X_NO_LEN)
	order := Order { O_ID : 1, O_D_ID : 1, O_W_ID : 1 }
	assert.Equal(encodeSlow(order), X_O_LEN)
	orderline := OrderLine { OL_O_ID : 1, OL_D_ID : 1, OL_W_ID : 1, OL_NUMBER : 1 }
	assert.Equal(encodeSlow(orderline), X_OL_LEN)
	item := Item { I_ID : 1 }
	assert.Equal(encodeSlow(item), X_I_LEN)
	stock := Stock { S_I_ID : 1, S_W_ID : 1 }
	assert.Equal(encodeSlow(stock), X_S_LEN)
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
	body := func(txn *vmvcc.Txn) bool {
		InsertWarehouse(
			txn,
			41,
			"name", "street1", "street2", "city",
			[2]byte{'M', 'A'}, [9]byte{'0', '2', '1', '3', '9'},
			6.25, 80.0,
		)
		return true
	}
	ok := txno.Run(body)
	assert.Equal(true, ok)

	/* Read it, update it, and read it again in one transaction. */
	body = func(txn *vmvcc.Txn) bool {
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
	ok = txno.Run(body)
	assert.Equal(true, ok)

	/* Read it again. */
	body = func(txn *vmvcc.Txn) bool {
		x, found := GetWarehouse(txn, 41)
		assert.Equal(true, found)
		assert.Equal(uint8(41), x.W_ID)
		assert.Equal(float32(6.25), x.W_TAX)
		assert.Equal(float32(90.0), x.W_YTD)
		return true
	}
	ok = txno.Run(body)
	assert.Equal(true, ok)
}

func TestTableDistrict(t *testing.T) {
	assert := assert.New(t)
	mgr := txn.MkTxnMgr()
	txno := mgr.New()

	/* Insert a District record. */
	body := func(txn *vmvcc.Txn) bool {
		InsertDistrict(
			txn,
			95, 41,
			"name", "street1", "street2", "city",
			[2]byte{'M', 'A'}, [9]byte{'0', '2', '1', '3', '9'},
			6.25, 80.0, 1, 1,
		)
		return true
	}
	ok := txno.Run(body)
	assert.Equal(true, ok)

	/* Read it, update it, and read it again in one transaction. */
	body = func(txn *vmvcc.Txn) bool {
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
	ok = txno.Run(body)
	assert.Equal(true, ok)

	/* Read it again. */
	body = func(txn *vmvcc.Txn) bool {
		x, found := GetDistrict(txn, 95, 41)
		assert.Equal(true, found)
		assert.Equal(float32(90.0), x.D_YTD)
		assert.Equal(uint32(2), x.D_NEXT_O_ID)
		assert.Equal(uint32(2), x.D_OLD_O_ID)
		return true
	}
	ok = txno.Run(body)
	assert.Equal(true, ok)
}

func TestTableCustomer(t *testing.T) {
	assert := assert.New(t)
	mgr := txn.MkTxnMgr()
	txno := mgr.New()

	/* Insert a Customer record. */
	body := func(txn *vmvcc.Txn) bool {
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
	ok := txno.Run(body)
	assert.Equal(true, ok)

	/* Read it, update it, and read it again in one transaction. */
	body = func(txn *vmvcc.Txn) bool {
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
	ok = txno.Run(body)
	assert.Equal(true, ok)

	/* Read it again. */
	body = func(txn *vmvcc.Txn) bool {
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
	ok = txno.Run(body)
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
	// var nLocalDistricts uint8 = N_DISTRICTS_PER_WAREHOUSE
	// var nLocalCustomers uint32 = N_CUSTOMERS_PER_DISTRICT
	// var nInitLocalNewOrders uint32 = N_INIT_NEW_ORDERS_PER_DISTRICT
	var nItems uint32 = 10
	var nWarehouses uint8 = 2
	var nLocalDistricts uint8 = 10
	var nLocalCustomers uint32 = 100
	var nInitLocalNewOrders uint32 = 30
	var nInitLocalOrders = nLocalCustomers
	assert.LessOrEqual(nInitLocalNewOrders, nInitLocalOrders)
	LoadTPCCSeq(
		txno,
		nItems, nWarehouses,
		nLocalDistricts, nLocalCustomers,
		nInitLocalNewOrders, 
	)

	/* Testing items. */
	body := func(txni *vmvcc.Txn) bool {
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
	ok = txno.Run(body)
	assert.Equal(true, ok)

	/* Testing Warehouse. */
	body = func(txni *vmvcc.Txn) bool {
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
	ok = txno.Run(body)
	assert.Equal(true, ok)

	/* Testing District. */
	body = func(txni *vmvcc.Txn) bool {
		var district *District
		var found bool
		for wid := uint8(0); wid <= nWarehouses + 1; wid++ {
			for did := uint8(0); did <= nLocalDistricts + 1; did++ {
				district, found = GetDistrict(txni, did, wid)
				if wid < 1 || wid > nWarehouses || did < 1 || did > nLocalDistricts {
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
	ok = txno.Run(body)
	assert.Equal(true, ok)

	/* Testing Customer. */
	body = func(txni *vmvcc.Txn) bool {
		/* For testing distribution. */
		var cntBCCustomers uint64 = 0
		var cntTotalCustomers uint64 = 0

		var customer *Customer
		var found bool
		for wid := uint8(0); wid <= nWarehouses + 1; wid++ {
			for did := uint8(0); did <= nLocalDistricts + 1; did++ {
				for cid := uint32(0); cid <= nLocalCustomers + 1; cid++ {
					customer, found = GetCustomer(txni, cid, did, wid)
					if wid < 1 || wid > nWarehouses ||
						did < 1 || did > nLocalDistricts ||
						cid < 1 || cid > nLocalCustomers {
						assert.Equal(false, found)
					} else {
						assert.Equal(true, found)
						assert.Equal(cid, customer.C_ID)
						assert.Equal(did, customer.C_D_ID)
						assert.Equal(wid, customer.C_W_ID)
						cntTotalCustomers++
						if customer.C_CREDIT == [2]byte{ 'B', 'C' } {
							cntBCCustomers++
						}
					}
				}
			}
		}

		/* Check that ~10% of customers have a bad credit. */
		ratioBC := float64(cntBCCustomers) / float64(cntTotalCustomers) * 100.0
		fmt.Printf("Ratio of customers with bad credits = %f%% (sholud be ~10%%).\n", ratioBC)
		return true
	}
	ok = txno.Run(body)
	assert.Equal(true, ok)

	/* Testing History. */
	body = func(txni *vmvcc.Txn) bool {
		var history *History
		var found bool
		var nHistory uint64 = uint64(nWarehouses) * uint64(nLocalDistricts) * uint64(nLocalCustomers)
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
				assert.LessOrEqual(history.H_D_ID, nLocalDistricts)
				assert.Less(uint32(0), history.H_C_ID)
				assert.LessOrEqual(history.H_C_ID, nLocalCustomers)
			}
		}
		return true
	}
	ok = txno.Run(body)
	assert.Equal(true, ok)

	/* Testing Order, NewOrder, and OrderLine. */
	body = func(txni *vmvcc.Txn) bool {
		/* For testing distribution. */
		var cntTotalItems uint64 = 0
		var cntRemoteItems uint64 = 0

		var order *Order
		var neworder *NewOrder
		var orderline *OrderLine
		var found bool
		for wid := uint8(0); wid <= nWarehouses + 1; wid++ {
			for did := uint8(0); did <= nLocalDistricts + 1; did++ {
				for oid := uint32(0); oid <= nInitLocalOrders + 1; oid++ {
					/* Order. */
					order, found = GetOrder(txni, oid, did, wid)
					if wid < 1 || wid > nWarehouses ||
						did < 1 || did > nLocalDistricts ||
						oid < 1 || oid > nInitLocalOrders {
						assert.Equal(false, found)
					} else {
						assert.Equal(true, found)
						assert.Equal(oid, order.O_ID)
						assert.Equal(did, order.O_D_ID)
						assert.Equal(wid, order.O_W_ID)
						assert.LessOrEqual(uint32(1), order.O_C_ID)
						assert.LessOrEqual(order.O_C_ID, nLocalCustomers)
						assert.LessOrEqual(OL_MIN_CNT, order.O_OL_CNT)
						assert.LessOrEqual(order.O_OL_CNT, OL_MAX_CNT)
					}

					/* NewOrder, (newoidlb, newoidub] are new orders. */
					newoidlb := nInitLocalOrders - nInitLocalNewOrders
					newoidub := nInitLocalOrders
					neworder, found = GetNewOrder(txni, oid, did, wid)
					if wid < 1 || wid > nWarehouses ||
						did < 1 || did > nLocalDistricts ||
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
					for olnum := uint8(0); olnum <= olcnt + 1; olnum++ {
						orderline, found = GetOrderLine(txni, oid, did, wid, olnum)
						if wid < 1 || wid > nWarehouses ||
							did < 1 || did > nLocalDistricts ||
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
	ok = txno.Run(body)
	assert.Equal(true, ok)

	/* Testing Stock. */
	body = func(txni *vmvcc.Txn) bool {
		var stock *Stock
		var found bool
		for wid := uint8(0); wid <= nWarehouses + 1; wid++ {
			for iid := uint32(0); iid <= nItems + 1; iid++ {
				stock, found = GetStock(txni, iid, wid)
				if wid < 1 || wid > nWarehouses || iid < 1 || iid > nItems {
					assert.Equal(false, found)
				} else {
					assert.Equal(true, found)
					assert.Equal(iid, stock.S_I_ID)
					assert.Equal(wid, stock.S_W_ID)
					assert.Equal(uint16(20), stock.S_QUANTITY)
				}
			}
		}

		return true
	}
	ok = txno.Run(body)
	assert.Equal(true, ok)
}


/**
 * Tests for TPC-C "business" transactions.
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
	body := func(txn *vmvcc.Txn) bool {
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
	ok = txno.Run(body)
	assert.Equal(true, ok)

	/* Run Payment transaction twice. */
	p := &PaymentInput{
		W_ID : wid,
		D_ID : did,
		H_AMOUNT : hamount,
		C_W_ID : cwid,
		C_D_ID : cdid,
		C_ID: cid,
	}
	ok = TxnPayment(txno, p)
	assert.Equal(true, ok)
	ok = TxnPayment(txno, p)
	assert.Equal(true, ok)
}
