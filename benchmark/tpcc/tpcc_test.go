package tpcc

import (
	"testing"
	"fmt"
	"github.com/stretchr/testify/assert"
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


func TestEncodeDecodeCustomer(t *testing.T) {
	assert := assert.New(t)
	c := Customer {
		C_ID : 14,
		C_W_ID : 223,
		C_LAST : [16]byte{4, 9},
	}
	s := encodeCustomer(&c)
	fmt.Printf("Customer record size = %d\n", len(s))
	d := decodeCustomer(s)
	assert.Equal(c, d)
}
