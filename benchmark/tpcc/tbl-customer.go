package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
	"github.com/mit-pdos/go-mvcc/txn"
)

/**
 * Convert primary keys of table Customer to a global key.
 * Used by all commands.
 */
func (c *Customer) gkey() uint64 {
	var gkey uint64 = uint64(c.C_ID)
	gkey = gkey << 8 + uint64(c.C_W_ID)
	gkey += TBLID_CUSTOMER
	return gkey
}

/**
 * Encode a Customer record to an opaque string.
 * Used by UPDATE/INSERT.
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
 * Used by SELECT.
 */
func (c *Customer) decode(opaque string) {
	err := binary.Read(strings.NewReader(opaque), binary.LittleEndian, c)
	if err != nil {
		log.Fatal("Decode error: ", err)
	}
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
 * Reader and writer operation invoking transation methods.
 */
func (c *Customer) Read(txn *txn.Txn) bool {
	gkey := c.gkey()
	opaque, found := txn.Get(gkey)
	/* TODO: check if we really need to do this check. */
	if !found {
		return false
	}
	c.decode(opaque)
	return true
}

func (c *Customer) Write(txn *txn.Txn) {
	gkey := c.gkey()
	s := c.encode()
	txn.Put(gkey, s)
}
