package tpcc

import (
	"testing"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/mit-pdos/go-mvcc/txn"
)

func TestTableId(t *testing.T) {
	fmt.Printf("%x\n", TBLID_WAREHOUSE)
	fmt.Printf("%x\n", TBLID_DISTRICT)
	fmt.Printf("%x\n", TBLID_CUSTOMER)
	fmt.Printf("%x\n", TBLID_HISTORY)
	fmt.Printf("%x\n", TBLID_NEWORDER)
	fmt.Printf("%x\n", TBLID_ORDER)
	fmt.Printf("%x\n", TBLID_ORDERLINE)
	fmt.Printf("%x\n", TBLID_ITEM)
	fmt.Printf("%x\n", TBLID_STOCK)
}

func TestEncodeCustomerKeys(t *testing.T) {
	c := &Customer {
		C_ID : 24,
		C_W_ID : 252,
	}
	fmt.Printf("%x\n", c.gkey())
}

func TestEncodeDecodeCustomer(t *testing.T) {
	assert := assert.New(t)
	c := &Customer {
		C_ID : 14,
		C_W_ID : 223,
		C_LAST : [16]byte{4, 9},
	}
	s := c.encode()
	fmt.Printf("Customer record size = %d\n", len(s))
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
		c := &Customer{
			C_ID : 20,
			C_W_ID : 41,
			C_BALANCE : 10.5,
			C_DATA : [500]byte{1, 3, 5},
		}
		c.Write(txn)
		return true
	}
	ok := txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Read it and update it. */
	body = func(txn *txn.Txn) bool {
		c := &Customer{
			C_ID : 20,
			C_W_ID : 41,
		}
		ok := c.Read(txn)
		assert.Equal(true, ok)
		assert.Equal(uint32(20), c.C_ID)
		assert.Equal(uint8(41), c.C_W_ID)
		assert.Equal(float32(10.5), c.C_BALANCE)
		assert.Equal([500]byte{1, 3, 5}, c.C_DATA)

		c.UpdateBadCredit(13.7, 16.66, 4, [500]byte{5, 4})
		c.Write(txn)
		return true
	}
	ok =  txno.DoTxn(body)
	assert.Equal(true, ok)

	/* Read it again. */
	body = func(txn *txn.Txn) bool {
		c := &Customer{
			C_ID : 20,
			C_W_ID : 41,
		}
		ok := c.Read(txn)
		assert.Equal(true, ok)
		assert.Equal(uint32(20), c.C_ID)
		assert.Equal(uint8(41), c.C_W_ID)
		assert.Equal(float32(13.7), c.C_BALANCE)
		assert.Equal(float32(16.66), c.C_YTD_PAYMENT)
		assert.Equal([500]byte{5, 4}, c.C_DATA)
		return true
	}
	ok =  txno.DoTxn(body)
	assert.Equal(true, ok)
}
