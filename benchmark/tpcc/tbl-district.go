package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
	"github.com/mit-pdos/go-mvcc/txn"
)

func GetDistrict(txn *txn.Txn, did uint8, wid uint8) *District {
	x := &District {
		D_ID   : did,
		D_W_ID : wid,
	}
	gkey := x.gkey()
	readtbl(txn, gkey, x)
	return x
}

func InsertDistrict(
	txn *txn.Txn,
	did uint8, wid uint8,
	name, street1, street2, city string,
	state [2]byte, zip [9]byte, tax, ytd float32,
	nextoid uint32,
) {
	x := &District {
		D_ID        : did,
		D_W_ID      : wid,
		D_STATE     : state,
		D_ZIP       : zip,
		D_TAX       : tax,
		D_YTD       : ytd,
		D_NEXT_O_ID : nextoid,
	}
	copy(x.D_NAME[:], name)
	copy(x.D_STREET_1[:], street1)
	copy(x.D_STREET_2[:], street2)
	copy(x.D_CITY[:], city)
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

func (x *District) IncrementNextOrderId(txn *txn.Txn) {
	x.D_NEXT_O_ID++
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

func (x *District) UpdateBalance(txn *txn.Txn, hamount float32) {
	x.D_YTD += hamount
	gkey := x.gkey()
	writetbl(txn, gkey, x)
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
