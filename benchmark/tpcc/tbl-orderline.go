package tpcc

import (
	"github.com/mit-pdos/go-mvcc/txn"
)

func GetOrderLine(
	txn *txn.Txn,
	oid uint32, did uint8, wid uint8, olnum uint8,
) (*OrderLine, bool) {
	x := &OrderLine {
		OL_O_ID   : oid,
		OL_D_ID   : did,
		OL_W_ID   : wid,
		OL_NUMBER : olnum,
	}
	gkey := x.gkey()
	found := readtbl(txn, gkey, x)
	return x, found
}

/**
 * Table mutator methods.
 */
func InsertOrderLine(
	txn *txn.Txn,
	oid uint32, did uint8, wid uint8, olnum uint8,
	iid uint32, iwid uint8, deliveryd uint32, quantity uint8,
	amount float32,
) {
	x := &OrderLine {
		OL_O_ID        : oid,
		OL_D_ID        : did,
		OL_W_ID        : wid,
		OL_NUMBER      : olnum,
		OL_I_ID        : iid,
		OL_SUPPLY_W_ID : iwid,
		OL_DELIVERY_D  : deliveryd,
		OL_QUANTITY    : quantity,
		OL_AMOUNT      : amount,
	}
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

func (x *OrderLine) UpdateDeliveryDate(txn *txn.Txn, deliveryd uint32) {
	x.OL_DELIVERY_D = deliveryd
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

/**
 * Convert primary keys of table OrderLine to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (x *OrderLine) gkey() uint64 {
	var gkey uint64 = uint64(x.OL_O_ID)
	gkey = gkey << 8 + uint64(x.OL_D_ID)
	gkey = gkey << 8 + uint64(x.OL_W_ID)
	gkey = gkey << 8 + uint64(x.OL_NUMBER)
	gkey += TBLID_ORDERLINE
	return gkey
}

/**
 * Encode a OrderLine record to an opaque string.
 * Used by TableWrite.
 */
func (x *OrderLine) encode() string {
	buf := make([]byte, X_OL_LEN)
	encodeU32(buf, x.OL_O_ID, X_OL_O_ID)
	encodeU8(buf, x.OL_D_ID, X_OL_D_ID)
	encodeU8(buf, x.OL_W_ID, X_OL_W_ID)
	encodeU8(buf, x.OL_NUMBER, X_OL_NUMBER)
	encodeU32(buf, x.OL_I_ID, X_OL_I_ID)
	encodeU8(buf, x.OL_SUPPLY_W_ID, X_OL_SUPPLY_W_ID)
	encodeU32(buf, x.OL_DELIVERY_D, X_OL_DELIVERY_D)
	encodeU8(buf, x.OL_QUANTITY, X_OL_QUANTITY)
	encodeF32(buf, x.OL_AMOUNT, X_OL_AMOUNT)
	/* this still creates a copy, but anyway*/
	return bytesToString(buf)
}

/**
 * Decode an opaque string to a OrderLine record.
 * Used by TableRead.
 */
func (x *OrderLine) decode(opaque string) {
	decodeU32(&x.OL_O_ID, opaque, X_OL_O_ID)
	decodeU8(&x.OL_D_ID, opaque, X_OL_D_ID)
	decodeU8(&x.OL_W_ID, opaque, X_OL_W_ID)
	decodeU8(&x.OL_NUMBER, opaque, X_OL_NUMBER)
	decodeU32(&x.OL_I_ID, opaque, X_OL_I_ID)
	decodeU8(&x.OL_SUPPLY_W_ID, opaque, X_OL_SUPPLY_W_ID)
	decodeU32(&x.OL_DELIVERY_D, opaque, X_OL_DELIVERY_D)
	decodeU8(&x.OL_QUANTITY, opaque, X_OL_QUANTITY)
	decodeF32(&x.OL_AMOUNT, opaque, X_OL_AMOUNT)
}
