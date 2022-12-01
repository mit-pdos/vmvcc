package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
)

func NewCustomer(cid uint32, cdid uint8, cwid uint8) *Customer {
	c := Customer {
		C_ID   : cid,
		C_D_ID : cdid,
		C_W_ID : cwid,
	}
	return &c
}

/**
 * Table mutator methods.
 */
func (c *Customer) UpdateBadCredit(
	bal float32, ytd float32, pcnt uint16, data [500]byte,
) {
	c.C_BALANCE = bal
	c.C_YTD_PAYMENT = ytd
	c.C_PAYMENT_CNT = pcnt
	c.C_DATA = data
}

/**
 * Convert primary keys of table Customer to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (c *Customer) gkey() uint64 {
	var gkey uint64 = uint64(c.C_ID)
	gkey = gkey << 8 + uint64(c.C_W_ID)
	gkey += TBLID_CUSTOMER
	return gkey
}

/**
 * Encode a Customer record to an opaque string.
 * Used by TableWrite.
 */
func (c *Customer) encode() string {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, *c)
	if err != nil {
		log.Fatal("Encode error: ", err)
	}
	return buf.String()
}

/**
 * Decode an opaque string to a Customer record.
 * Used by TableRead.
 */
func (c *Customer) decode(opaque string) {
	err := binary.Read(strings.NewReader(opaque), binary.LittleEndian, c)
	if err != nil {
		log.Fatal("Decode error: ", err)
	}
}
