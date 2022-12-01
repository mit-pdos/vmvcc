package tpcc

import (
	"bytes"
	"strings"
	"encoding/gob"
	"log"
	"github.com/mit-pdos/go-mvcc/txn"
)

/**
 * Convert primary keys of table Customer to a global key.
 * Used by all commands.
 */
func encodeCustomerKeys(cid uint32, cwid uint8) uint64 {
	var gkey uint64 = uint64(cid)
	gkey = gkey << 8 + uint64(cwid)
	gkey += TBLID_CUSTOMER
	return gkey
}

/**
 * Encode a Customer record to an opaque string.
 * Used by UPDATE/INSERT.
 *
 * Using Go's encoding library for now. Write my own if slow.
 */
func encodeCustomer(c *Customer) string {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(*c)
	if err != nil {
		log.Fatal("Encode error: ", err)
	}
	return buf.String()
}

/**
 * Decode an opaque string to a Customer record.
 * Used by SELECT.
 */
func decodeCustomer(opaque string) Customer {
	var c Customer
	dec := gob.NewDecoder(strings.NewReader(opaque))
	err := dec.Decode(&c)
	if err != nil {
		log.Fatal("Decode error: ", err)
	}
	return c
}

/**
 * Select always take a txn pointer, and primary keys of the table.
 */
func SelectCustomer(txn *txn.Txn, cid uint32, cwid uint8) (Customer, bool) {
	gkey := encodeCustomerKeys(cid, cwid)
	opaque, found := txn.Get(gkey)
	/* TODO: check if we need to do this check. */
	if !found {
		return Customer{}, false
	}
	customer := decodeCustomer(opaque)
	return customer, true
}

/**
 * Update always take a txn pointer, a table struct pointer, and new field values.
 */
func UpdateCustomerBadCredit(
	txn *txn.Txn, c *Customer,
	bal float32, ytd float32, pcnt uint16, data [500]byte) {
	gkey := encodeCustomerKeys(c.C_ID, c.C_W_ID)
	// TODO: update each field of c
	s := encodeCustomer(c)
	txn.Put(gkey, s)
}
