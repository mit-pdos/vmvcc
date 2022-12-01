package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
)

func NewCustomer(cid uint32, cdid uint8, cwid uint8) *Customer {
	c := Customer {
		C_ID   : cid,
		C_D_ID : cdid,
		C_W_ID : cwid,
	}
	return &c
}

/**
 * Table mutator methods.
 */
func (x *Customer) UpdateBadCredit(
	bal float32, ytd float32, pcnt uint16, data [500]byte,
) {
	x.C_BALANCE = bal
	x.C_YTD_PAYMENT = ytd
	x.C_PAYMENT_CNT = pcnt
	x.C_DATA = data
}

/**
 * Convert primary keys of table Customer to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (x *Customer) gkey() uint64 {
	var gkey uint64 = uint64(x.C_ID)
	gkey = gkey << 8 + uint64(x.C_D_ID)
	gkey = gkey << 8 + uint64(x.C_W_ID)
	gkey += TBLID_CUSTOMER
	return gkey
}

/**
 * Encode a Customer record to an opaque string.
 * Used by TableWrite.
 */
func (x *Customer) encode() string {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Encode error: ", err)
	}
	return buf.String()
}

/**
 * Decode an opaque string to a Customer record.
 * Used by TableRead.
 */
func (x *Customer) decode(opaque string) {
	err := binary.Read(strings.NewReader(opaque), binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Decode error: ", err)
	}
}
