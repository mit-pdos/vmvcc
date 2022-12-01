package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
)

func NewCustomer(cid uint32, cdid uint8, cwid uint8) *Customer {
	x := Customer {
		C_ID   : cid,
		C_D_ID : cdid,
		C_W_ID : cwid,
	}
	return &x
}

/**
 * Table mutator methods.
 */
func (x *Customer) UpdateOnBadCredit(hamount float32, cdata string) {
	x.C_BALANCE -= hamount
	x.C_YTD_PAYMENT += hamount
	x.C_PAYMENT_CNT++
	copy(x.C_DATA[:], cdata)
}

func (x *Customer) UpdateOnGoodCredit(hamount float32) {
	x.C_BALANCE -= hamount
	x.C_YTD_PAYMENT += hamount
	x.C_PAYMENT_CNT++
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