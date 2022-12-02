package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
)

func NewOrderLine(oid uint32, did uint8, wid uint8, olnum uint8) *OrderLine {
	x := OrderLine {
		OL_O_ID   : oid,
		OL_D_ID   : did,
		OL_W_ID   : wid,
		OL_NUMBER : olnum,
	}
	return &x
}

/**
 * Table mutator methods.
 */
func (x *OrderLine) Initialize(
	iid uint32, iwid uint8, deliveryd uint32, quantity uint8,
	amount float32, distinfo [24]byte,
) {

	x.OL_I_ID        = iid
	x.OL_SUPPLY_W_ID = iwid
	x.OL_DELIVERY_D  = deliveryd
	x.OL_QUANTITY    = quantity
	x.OL_AMOUNT      = amount
	x.OL_DIST_INFO   = distinfo
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
	buf := new(bytes.Buffer)
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
