package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
)

func NewWarehouse(wid uint8) *Warehouse {
	x := Warehouse { W_ID : wid }
	return &x
}

/**
 * Table mutator methods.
 */
func (x *Warehouse) UpdateBalance(hamount float32) {
	x.W_YTD += hamount
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
