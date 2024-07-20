package main

import (
	"github.com/mit-pdos/vmvcc/vmvcc"
)

func GetWarehouse(txn *vmvcc.Txn, wid uint8) (*Warehouse, bool) {
	x := &Warehouse{W_ID: wid}
	gkey := x.gkey()
	found := readtbl(txn, gkey, x)
	return x, found
}

func InsertWarehouse(
	txn *vmvcc.Txn,
	wid uint8,
	name, street1, street2, city string,
	state [2]byte, zip [9]byte, tax, ytd float32,
) {
	x := &Warehouse{
		W_ID:    wid,
		W_STATE: state,
		W_ZIP:   zip,
		W_TAX:   tax,
		W_YTD:   ytd,
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
func (x *Warehouse) UpdateBalance(txn *vmvcc.Txn, hamount float32) {
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
	buf := make([]byte, X_W_LEN)
	encodeU8(buf, x.W_ID, X_W_ID)
	encodeBytes(buf, x.W_NAME[:], X_W_NAME)
	encodeBytes(buf, x.W_STREET_1[:], X_W_STREET_1)
	encodeBytes(buf, x.W_STREET_2[:], X_W_STREET_2)
	encodeBytes(buf, x.W_CITY[:], X_W_CITY)
	encodeBytes(buf, x.W_STATE[:], X_W_STATE)
	encodeBytes(buf, x.W_ZIP[:], X_W_ZIP)
	encodeF32(buf, x.W_TAX, X_W_TAX)
	encodeF32(buf, x.W_YTD, X_W_YTD)
	return bytesToString(buf)
}

/**
 * Decode an opaque string to a Warehouse record.
 * Used by TableRead.
 */
func (x *Warehouse) decode(opaque string) {
	decodeU8(&x.W_ID, opaque, X_W_ID)
	decodeString(x.W_NAME[:], opaque, X_W_NAME)
	decodeString(x.W_STREET_1[:], opaque, X_W_STREET_1)
	decodeString(x.W_STREET_2[:], opaque, X_W_STREET_2)
	decodeString(x.W_CITY[:], opaque, X_W_CITY)
	decodeString(x.W_STATE[:], opaque, X_W_STATE)
	decodeString(x.W_ZIP[:], opaque, X_W_ZIP)
	decodeF32(&x.W_TAX, opaque, X_W_TAX)
	decodeF32(&x.W_YTD, opaque, X_W_YTD)
}
