package tpcc

import (
	"testing"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/mit-pdos/go-mvcc/txn"
)

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
}

func TestGkey(t *testing.T) {
	warehouse := Warehouse { W_ID : 1 }
	fmt.Printf("%.15x\n", warehouse.gkey())
	district := District { D_ID : 1, D_W_ID : 1 }
	fmt.Printf("%x\n", district.gkey())
	customer := Customer { C_ID : 1, C_D_ID: 1, C_W_ID: 1 }
	fmt.Printf("%x\n", customer.gkey())
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

func TestCustomerTxn(t *testing.T) {
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
		// c := NewCustomer(20, 95, 41)
		// c.C_BALANCE = 60.0
		// c.C_YTD_PAYMENT = 80.0
		// c.C_PAYMENT_CNT = 3
		// c.C_DATA = ""
		return true
	}
	ok := txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Read it and update it. */
	body = func(txn *txn.Txn) bool {
		c := GetCustomer(txn, 20, 95, 41)
		assert.Equal(true, ok)
		assert.Equal(uint32(20), c.C_ID)
		assert.Equal(uint8(95), c.C_D_ID)
		assert.Equal(uint8(41), c.C_W_ID)
		assert.Equal(float32(60.0), c.C_BALANCE)
		assert.Equal(float32(80.0), c.C_YTD_PAYMENT)
		assert.Equal(uint16(3), c.C_PAYMENT_CNT)
		// assert.Equal("", c.C_DATA)

		c.UpdateOnBadCredit(txn, 10.0, "Hello Customer")
		return true
	}
	ok = txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Read it again. */
	body = func(txn *txn.Txn) bool {
		c := GetCustomer(txn, 20, 95, 41)
		assert.Equal(true, ok)
		assert.Equal(uint32(20), c.C_ID)
		assert.Equal(uint8(95), c.C_D_ID)
		assert.Equal(uint8(41), c.C_W_ID)
		assert.Equal(float32(50.0), c.C_BALANCE)
		assert.Equal(float32(90.0), c.C_YTD_PAYMENT)
		assert.Equal(uint16(4), c.C_PAYMENT_CNT)
		// assert.Equal("1 2 3 4 5|", c.C_DATA)
		return true
	}
	ok = txno.DoTxn(body)
	assert.Equal(true, ok)
}

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
		// c.C_BALANCE = 60.0
		// c.C_YTD_PAYMENT = 80.0
		// c.C_PAYMENT_CNT = 3
		// c.C_CREDIT = [2]byte{'B', 'C'}
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
