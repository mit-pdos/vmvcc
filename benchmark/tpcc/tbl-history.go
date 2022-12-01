package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
)

func NewHistory(hid uint64) *History {
	x := History { H_ID : hid }
	return &x
}

func (x *History) Initialize(
	cid uint32, cdid uint8, cwid uint8, did uint8, wid uint8,
	date uint32, hamount float32, hdata string,
) {
	x.H_C_ID = cid
	x.H_C_D_ID = cdid
	x.H_C_W_ID = cwid
	x.H_D_ID = did
	x.H_W_ID = wid
	x.H_DATE = date
	x.H_AMOUNT = hamount
	copy(x.H_DATA[:], hdata)
}

/**
 * Convert primary keys of table History to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (x *History) gkey() uint64 {
	var gkey uint64 = x.H_ID
	gkey += TBLID_HISTORY
	return gkey
}

/**
 * Encode a History record to an opaque string.
 * Used by TableWrite.
 */
func (x *History) encode() string {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Encode error: ", err)
	}
	return buf.String()
}

/**
 * Decode an opaque string to a History record.
 * Used by TableRead.
 */
func (x *History) decode(opaque string) {
	err := binary.Read(strings.NewReader(opaque), binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Decode error: ", err)
	}
}
