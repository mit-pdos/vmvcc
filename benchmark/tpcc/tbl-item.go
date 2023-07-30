package main

import (
	"github.com/mit-pdos/vmvcc/vmvcc"
)

func GetItem(txn *vmvcc.Txn, iid uint32) (*Item, bool) {
	x := &Item { I_ID : iid }
	gkey := x.gkey()
	found := readtbl(txn, gkey, x)
	return x, found
}

/**
 * Table mutator methods.
 */
func InsertItem(
	txn *vmvcc.Txn,
	iid uint32,
	imid uint32, name string, price float32, data string,
) {
	x := &Item {
		I_ID    : iid,
		I_IM_ID : imid,
		I_PRICE : price,
	}
	copy(x.I_NAME[:], name)
	copy(x.I_DATA[:], data)
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

/**
 * Convert primary keys of table Item to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (x *Item) gkey() uint64 {
	var gkey uint64 = uint64(x.I_ID)
	gkey += TBLID_ITEM
	return gkey
}

/**
 * Encode a Item record to an opaque string.
 * Used by TableWrite.
 */
func (x *Item) encode() string {
	buf := make([]byte, X_I_LEN)
	encodeU32(buf, x.I_ID, X_I_ID)
	encodeU32(buf, x.I_IM_ID, X_I_IM_ID)
	encodeBytes(buf, x.I_NAME[:], X_I_NAME)
	encodeF32(buf, x.I_PRICE, X_I_PRICE)
	encodeBytes(buf, x.I_DATA[:], X_I_DATA)
	return bytesToString(buf)
}

/**
 * Decode an opaque string to a Item record.
 * Used by TableRead.
 */
func (x *Item) decode(opaque string) {
	decodeU32(&x.I_ID, opaque, X_I_ID)
	decodeU32(&x.I_IM_ID, opaque, X_I_IM_ID)
	decodeString(x.I_NAME[:], opaque, X_I_NAME)
	decodeF32(&x.I_PRICE, opaque, X_I_PRICE)
	decodeString(x.I_DATA[:], opaque, X_I_DATA)
}
