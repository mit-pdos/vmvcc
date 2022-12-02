package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
)

func NewOrderRecord(oid uint32, did uint8, wid uint8) *Order {
	x := Order {
		O_ID   : oid,
		O_D_ID : did,
		O_W_ID : wid,
	}
	return &x
}

/**
 * Table mutator methods.
 */
func (x *Order) Initialize(
	cid uint32, oentryd uint32, ocarrierid uint8, olcnt uint8, alllocal bool,
) {
	x.O_C_ID = cid
	x.O_ENTRY_D = oentryd
	x.O_CARRIER_ID = ocarrierid
	x.O_OL_CNT = olcnt
	x.O_ALL_LOCAL = alllocal
}

/**
 * Convert primary keys of table Order to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (x *Order) gkey() uint64 {
	var gkey uint64 = uint64(x.O_ID)
	gkey = gkey << 8 + uint64(x.O_D_ID)
	gkey = gkey << 8 + uint64(x.O_W_ID)
	gkey += TBLID_ORDER
	return gkey
}

/**
 * Encode a Order record to an opaque string.
 * Used by TableWrite.
 */
func (x *Order) encode() string {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Encode error: ", err)
	}
	return buf.String()
}

/**
 * Decode an opaque string to a Order record.
 * Used by TableRead.
 */
func (x *Order) decode(opaque string) {
	err := binary.Read(strings.NewReader(opaque), binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Decode error: ", err)
	}
}
