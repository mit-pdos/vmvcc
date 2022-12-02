package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
)

func NewStock(iid uint32, wid uint8) *Stock {
	x := Stock {
		S_I_ID : iid,
		S_W_ID : wid,
	}
	return &x
}

func (x *Stock) Update(
	quantity uint16, ytd uint32, ordercnt uint16, remotecnt uint16,
) {
	x.S_QUANTITY   = quantity
	x.S_YTD        = ytd
	x.S_ORDER_CNT  = ordercnt
	x.S_REMOTE_CNT = remotecnt
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
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Encode error: ", err)
	}
	return buf.String()
}

/**
 * Decode an opaque string to a Stock record.
 * Used by TableRead.
 */
func (x *Stock) decode(opaque string) {
	err := binary.Read(strings.NewReader(opaque), binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Decode error: ", err)
	}
}
