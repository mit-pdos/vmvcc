package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
	"github.com/mit-pdos/go-mvcc/txn"
)

func GetHistory(txn *txn.Txn, hid uint64) (*History, bool) {
	x := &History { H_ID : hid }
	gkey := x.gkey()
	found := readtbl(txn, gkey, x)
	return x, found
}

func InsertHistory(
	txn *txn.Txn,
	hid uint64,
	cid uint32, cdid uint8, cwid uint8, did uint8, wid uint8,
	date uint32, hamount float32, hdata string,
) {
	x := &History {
		H_ID     : hid,
		H_C_ID   : cid,
		H_C_D_ID : cdid,
		H_C_W_ID : cwid,
		H_D_ID   : did,
		H_W_ID   : wid,
		H_DATE   : date,
		H_AMOUNT : hamount,
	}
	copy(x.H_DATA[:], hdata)
	gkey := x.gkey()
	writetbl(txn, gkey, x)
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
