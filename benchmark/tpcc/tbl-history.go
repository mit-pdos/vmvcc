package main

import (
	"github.com/mit-pdos/vmvcc/vmvcc"
)

func GetHistory(txn *vmvcc.Txn, hid uint64) (*History, bool) {
	x := &History{H_ID: hid}
	gkey := x.gkey()
	found := readtbl(txn, gkey, x)
	return x, found
}

func InsertHistory(
	txn *vmvcc.Txn,
	hid uint64,
	cid uint32, cdid uint8, cwid uint8, did uint8, wid uint8,
	date uint32, hamount float32, hdata string,
) {
	x := &History{
		H_ID:     hid,
		H_C_ID:   cid,
		H_C_D_ID: cdid,
		H_C_W_ID: cwid,
		H_D_ID:   did,
		H_W_ID:   wid,
		H_DATE:   date,
		H_AMOUNT: hamount,
	}
	copy(x.H_DATA[:], hdata)
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

/**
 * Convert primary keys of table History to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (x *History) gkey() uint64 {
	var gkey uint64 = x.H_ID
	gkey += TBLID_HISTORY
	return gkey
}

/**
 * Encode a History record to an opaque string.
 * Used by TableWrite.
 */
func (x *History) encode() string {
	buf := make([]byte, X_H_LEN)
	encodeU64(buf, x.H_ID, X_H_ID)
	encodeU32(buf, x.H_C_ID, X_H_C_ID)
	encodeU8(buf, x.H_C_D_ID, X_H_C_D_ID)
	encodeU8(buf, x.H_C_W_ID, X_H_C_W_ID)
	encodeU8(buf, x.H_D_ID, X_H_D_ID)
	encodeU8(buf, x.H_W_ID, X_H_W_ID)
	encodeU32(buf, x.H_DATE, X_H_DATE)
	encodeF32(buf, x.H_AMOUNT, X_H_AMOUNT)
	encodeBytes(buf, x.H_DATA[:], X_H_DATA)
	return bytesToString(buf)
}

/**
 * Decode an opaque string to a History record.
 * Used by TableRead.
 */
func (x *History) decode(opaque string) {
	decodeU64(&x.H_ID, opaque, X_H_ID)
	decodeU32(&x.H_C_ID, opaque, X_H_C_ID)
	decodeU8(&x.H_C_D_ID, opaque, X_H_C_D_ID)
	decodeU8(&x.H_C_W_ID, opaque, X_H_C_W_ID)
	decodeU8(&x.H_D_ID, opaque, X_H_D_ID)
	decodeU8(&x.H_W_ID, opaque, X_H_W_ID)
	decodeU32(&x.H_DATE, opaque, X_H_DATE)
	decodeF32(&x.H_AMOUNT, opaque, X_H_AMOUNT)
	decodeString(x.H_DATA[:], opaque, X_H_DATA)
}
