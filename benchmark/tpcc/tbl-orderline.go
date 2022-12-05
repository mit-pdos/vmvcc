package tpcc

import (
	"strings"
	"encoding/binary"
	"log"
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
	buf := new(strings.Builder)
	buf.Grow(int(X_OL_LEN))
	err := binary.Write(buf, binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Encode error: ", err)
	}
	return buf.String()
}

/**
 * Decode an opaque string to a OrderLine record.
 * Used by TableRead.
 */
func (x *OrderLine) decode(opaque string) {
	err := binary.Read(strings.NewReader(opaque), binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Decode error: ", err)
	}
}
