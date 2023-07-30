package main

import (
	"github.com/mit-pdos/vmvcc/vmvcc"
)

func GetStock(txn *vmvcc.Txn, iid uint32, wid uint8) (*Stock, bool) {
	x := &Stock {
		S_I_ID : iid,
		S_W_ID : wid,
	}
	gkey := x.gkey()
	found := readtbl(txn, gkey, x)
	return x, found
}

func InsertStock(
	txn *vmvcc.Txn,
	iid uint32, wid uint8,
	quantity uint16, ytd uint32,
	ordercnt, remotecnt uint16, data string,
) {
	x := &Stock {
		S_I_ID       : iid,
		S_W_ID       : wid,
		S_QUANTITY   : quantity,
		S_YTD        : ytd,
		S_ORDER_CNT  : ordercnt,
		S_REMOTE_CNT : remotecnt,
	}
	copy(x.S_DATA[:], data)
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

func (x *Stock) Update(
	txn *vmvcc.Txn,
	quantity uint16, ytd uint32, ordercnt uint16, remotecnt uint16,
) {
	x.S_QUANTITY   = quantity
	x.S_YTD        = ytd
	x.S_ORDER_CNT  = ordercnt
	x.S_REMOTE_CNT = remotecnt
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

/**
 * Convert primary keys of table Stock to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (x *Stock) gkey() uint64 {
	var gkey uint64 = uint64(x.S_I_ID)
	gkey = gkey << 8 + uint64(x.S_W_ID)
	gkey += TBLID_STOCK
	return gkey
}

/**
 * Encode a Stock record to an opaque string.
 * Used by TableWrite.
 */
func (x *Stock) encode() string {
	buf := make([]byte, X_S_LEN)
	encodeU32(buf, x.S_I_ID, X_S_I_ID)
	encodeU8(buf, x.S_W_ID, X_S_W_ID)
	encodeU16(buf, x.S_QUANTITY, X_S_QUANTITY)
	encodeU32(buf, x.S_YTD, X_S_YTD)
	encodeU16(buf, x.S_ORDER_CNT, X_S_ORDER_CNT)
	encodeU16(buf, x.S_REMOTE_CNT, X_S_REMOTE_CNT)
	encodeBytes(buf, x.S_DATA[:], X_S_DATA)
	return bytesToString(buf)
}

/**
 * Decode an opaque string to a Stock record.
 * Used by TableRead.
 */
func (x *Stock) decode(opaque string) {
	decodeU32(&x.S_I_ID, opaque, X_S_I_ID)
	decodeU8(&x.S_W_ID, opaque, X_S_W_ID)
	decodeU16(&x.S_QUANTITY, opaque, X_S_QUANTITY)
	decodeU32(&x.S_YTD, opaque, X_S_YTD)
	decodeU16(&x.S_ORDER_CNT, opaque, X_S_ORDER_CNT)
	decodeU16(&x.S_REMOTE_CNT, opaque, X_S_REMOTE_CNT)
	decodeString(x.S_DATA[:], opaque, X_S_DATA)
}
