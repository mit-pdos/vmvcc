package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
	"github.com/mit-pdos/go-mvcc/txn"
)

func GetWarehouse(txn *txn.Txn, wid uint8) *Warehouse {
	x := &Warehouse { W_ID : wid }
	gkey := x.gkey()
	readtbl(txn, gkey, x)
	return x
}

func InsertWarehouse(
	txn *txn.Txn,
	wid uint8,
	name, street1, street2, city string,
	state [2]byte, zip [9]byte, tax, ytd float32,
) {
	x := &Warehouse {
		W_ID    : wid,
		W_STATE : state,
		W_ZIP   : zip,
		W_TAX   : tax,
		W_YTD   : ytd,
	}
	copy(x.W_NAME[:], name)
	copy(x.W_STREET_1[:], street1)
	copy(x.W_STREET_2[:], street2)
	copy(x.W_CITY[:], city)
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

/**
 * Table mutator methods.
 */
func (x *Warehouse) UpdateBalance(txn *txn.Txn, hamount float32) {
	x.W_YTD += hamount
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

/**
 * Convert primary keys of table Warehouse to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (x *Warehouse) gkey() uint64 {
	var gkey uint64 = uint64(x.W_ID)
	gkey += TBLID_WAREHOUSE
	return gkey
}

/**
 * Encode a Warehouse record to an opaque string.
 * Used by TableWrite.
 */
func (x *Warehouse) encode() string {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Encode error: ", err)
	}
	return buf.String()
}

/**
 * Decode an opaque string to a Warehouse record.
 * Used by TableRead.
 */
func (x *Warehouse) decode(opaque string) {
	err := binary.Read(strings.NewReader(opaque), binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Decode error: ", err)
	}
}
