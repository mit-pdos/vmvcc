package main

import (
	"github.com/mit-pdos/vmvcc/vmvcc"
)

func GetOrder(txn *vmvcc.Txn, oid uint32, did uint8, wid uint8) (*Order, bool) {
	x := &Order{
		O_ID:   oid,
		O_D_ID: did,
		O_W_ID: wid,
	}
	gkey := x.gkey()
	found := readtbl(txn, gkey, x)
	return x, found
}

func GetOrdersByIndex(
	txn *vmvcc.Txn,
	cid uint32, did uint8, wid uint8,
) []*Order {
	records := make([]*Order, 0, 10)

	/* Read the index entry. */
	x := &Order{
		O_C_ID: cid,
		O_D_ID: did,
		O_W_ID: wid,
	}
	gkeyidx := x.gkeyidx()
	gkeys, found := readidx(txn, gkeyidx)
	if !found {
		return records
	}

	/* Read all the records. */
	for _, gkey := range gkeys {
		r := new(Order)
		readtbl(txn, gkey, r)
		records = append(records, r)
	}

	return records
}

/**
 * Table mutator methods.
 */
func InsertOrder(
	txn *vmvcc.Txn,
	oid uint32, did uint8, wid uint8,
	cid uint32, oentryd uint32, ocarrierid uint8, olcnt uint8, alllocal bool,
) {
	x := &Order{
		O_ID:         oid,
		O_D_ID:       did,
		O_W_ID:       wid,
		O_C_ID:       cid,
		O_ENTRY_D:    oentryd,
		O_CARRIER_ID: ocarrierid,
		O_OL_CNT:     olcnt,
		O_ALL_LOCAL:  alllocal,
	}
	gkey := x.gkey()
	writetbl(txn, gkey, x)

	/* Update index. */
	gkeyidx := x.gkeyidx()
	ents, found := readidx(txn, gkeyidx)
	if !found {
		ents = make([]uint64, 0)
	}
	ents = append(ents, gkey)
	writeidx(txn, gkeyidx, ents)
}

func (x *Order) UpdateCarrier(txn *vmvcc.Txn, ocarrierid uint8) {
	x.O_CARRIER_ID = ocarrierid
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

/**
 * Convert primary keys of table Order to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (x *Order) gkey() uint64 {
	var gkey uint64 = uint64(x.O_ID)
	gkey = gkey<<8 + uint64(x.O_D_ID)
	gkey = gkey<<8 + uint64(x.O_W_ID)
	gkey += TBLID_ORDER
	return gkey
}

func (x *Order) gkeyidx() uint64 {
	var gkey uint64 = uint64(x.O_C_ID)
	gkey = gkey<<8 + uint64(x.O_D_ID)
	gkey = gkey<<8 + uint64(x.O_W_ID)
	gkey += IDXID_ORDER
	return gkey
}

/**
 * Encode a Order record to an opaque string.
 * Used by TableWrite.
 */
func (x *Order) encode() string {
	buf := make([]byte, X_O_LEN)
	encodeU32(buf, x.O_ID, X_O_ID)
	encodeU8(buf, x.O_D_ID, X_O_D_ID)
	encodeU8(buf, x.O_W_ID, X_O_W_ID)
	encodeU32(buf, x.O_C_ID, X_O_C_ID)
	encodeU32(buf, x.O_ENTRY_D, X_O_ENTRY_D)
	encodeU8(buf, x.O_CARRIER_ID, X_O_CARRIER_ID)
	encodeU8(buf, x.O_OL_CNT, X_O_OL_CNT)
	encodeBool(buf, x.O_ALL_LOCAL, X_O_ALL_LOCAL)
	return bytesToString(buf)
}

/**
 * Decode an opaque string to a Order record.
 * Used by TableRead.
 */
func (x *Order) decode(opaque string) {
	decodeU32(&x.O_ID, opaque, X_O_ID)
	decodeU8(&x.O_D_ID, opaque, X_O_D_ID)
	decodeU8(&x.O_W_ID, opaque, X_O_W_ID)
	decodeU32(&x.O_C_ID, opaque, X_O_C_ID)
	decodeU32(&x.O_ENTRY_D, opaque, X_O_ENTRY_D)
	decodeU8(&x.O_CARRIER_ID, opaque, X_O_CARRIER_ID)
	decodeU8(&x.O_OL_CNT, opaque, X_O_OL_CNT)
	decodeBool(&x.O_ALL_LOCAL, opaque, X_O_ALL_LOCAL)
}
