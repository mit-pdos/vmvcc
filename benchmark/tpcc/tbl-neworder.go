package tpcc

import (
	"github.com/mit-pdos/vmvcc/txn"
)

func GetNewOrder(txn *txn.Txn, oid uint32, did uint8, wid uint8) (*NewOrder, bool) {
	x := &NewOrder {
		NO_O_ID : oid,
		NO_D_ID : did,
		NO_W_ID : wid,
	}
	gkey := x.gkey()
	found := readtbl(txn, gkey, x)
	return x, found
}

/**
 * Table mutator methods.
 */
func InsertNewOrder(txn *txn.Txn, oid uint32, did uint8, wid uint8) {
	x := &NewOrder {
		NO_O_ID : oid,
		NO_D_ID : did,
		NO_W_ID : wid,
	}
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

func DeleteNewOrder(txn *txn.Txn, oid uint32, did uint8, wid uint8) {
	x := &NewOrder {
		NO_O_ID : oid,
		NO_D_ID : did,
		NO_W_ID : wid,
	}
	gkey := x.gkey()
	deletetbl(txn, gkey)
}

/**
 * Convert primary keys of table NewOrder to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (x *NewOrder) gkey() uint64 {
	var gkey uint64 = uint64(x.NO_O_ID)
	gkey = gkey << 8 + uint64(x.NO_D_ID)
	gkey = gkey << 8 + uint64(x.NO_W_ID)
	gkey += TBLID_NEWORDER
	return gkey
}

/**
 * Encode a NewOrder record to an opaque string.
 * Used by TableWrite.
 */
func (x *NewOrder) encode() string {
	buf := make([]byte, X_NO_LEN)
	encodeU32(buf, x.NO_O_ID, X_NO_O_ID)
	encodeU8(buf, x.NO_D_ID, X_NO_D_ID)
	encodeU8(buf, x.NO_W_ID, X_NO_W_ID)
	return bytesToString(buf)
}

/**
 * Decode an opaque string to a NewOrder record.
 * Used by TableRead.
 */
func (x *NewOrder) decode(opaque string) {
	decodeU32(&x.NO_O_ID, opaque, X_NO_O_ID)
	decodeU8(&x.NO_D_ID, opaque, X_NO_D_ID)
	decodeU8(&x.NO_W_ID, opaque, X_NO_W_ID)
}
