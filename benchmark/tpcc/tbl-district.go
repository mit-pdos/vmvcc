package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
)

func NewDistrict(did uint8, dwid uint8) *District {
	x := District {
		D_ID   : did,
		D_W_ID : dwid,
	}
	return &x
}

func (x *District) IncrementNextOrderId() {
	x.D_NEXT_O_ID++
}

func (x *District) UpdateBalance(hamount float32) {
	x.D_YTD += hamount
}

/**
 * Convert primary keys of table District to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (x *District) gkey() uint64 {
	var gkey uint64 = uint64(x.D_ID)
	gkey = gkey << 8 + uint64(x.D_W_ID)
	gkey += TBLID_DISTRICT
	return gkey
}

/**
 * Encode a District record to an opaque string.
 * Used by TableWrite.
 */
func (x *District) encode() string {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Encode error: ", err)
	}
	return buf.String()
}

/**
 * Decode an opaque string to a District record.
 * Used by TableRead.
 */
func (x *District) decode(opaque string) {
	err := binary.Read(strings.NewReader(opaque), binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Decode error: ", err)
	}
}
