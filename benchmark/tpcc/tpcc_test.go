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
	warehouse := NewWarehouse(1)
	fmt.Printf("%.15x\n", warehouse.gkey())
	district := NewDistrict(1, 1)
	fmt.Printf("%x\n", district.gkey())
	customer := NewCustomer(1, 1, 1)
	fmt.Printf("%x\n", customer.gkey())
}

func TestRecordSize(t *testing.T) {
	var s string
	warehouse := NewWarehouse(1)
	s = warehouse.encode()
	fmt.Printf("Warehouse record size = %d\n", len(s))
	district := NewDistrict(1, 1)
	s = district.encode()
	fmt.Printf("District record size = %d\n", len(s))
	customer := NewCustomer(1, 1, 1)
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
		c := NewCustomer(20, 95, 41)
		c.C_BALANCE = 60.0
		c.C_YTD_PAYMENT = 80.0
		c.C_PAYMENT_CNT = 3
		// c.C_DATA = ""
		WriteTable(c, txn)
		return true
	}
	ok := txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Read it and update it. */
	body = func(txn *txn.Txn) bool {
		c := NewCustomer(20, 95, 41)
		ok := ReadTable(c, txn)
		assert.Equal(true, ok)
		assert.Equal(uint32(20), c.C_ID)
		assert.Equal(uint8(95), c.C_D_ID)
		assert.Equal(uint8(41), c.C_W_ID)
		assert.Equal(float32(60.0), c.C_BALANCE)
		assert.Equal(float32(80.0), c.C_YTD_PAYMENT)
		assert.Equal(uint16(3), c.C_PAYMENT_CNT)
		// assert.Equal("", c.C_DATA)

		c.UpdateBadCredit(1, 2, 3, 4, 5, 10.0)
		WriteTable(c, txn)
		return true
	}
	ok = txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Read it again. */
	body = func(txn *txn.Txn) bool {
		c := NewCustomer(20, 95, 41)
		ok := ReadTable(c, txn)
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

	ok := TxnPayment(txno, cid, cdid, cwid, did, wid, hamount)
	assert.Equal(true, ok)
}
