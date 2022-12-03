package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
	"github.com/mit-pdos/go-mvcc/txn"
)

func GetNewOrder(txn *txn.Txn, oid uint32, did uint8, wid uint8) (*NewOrder, bool) {
	x := &NewOrder {
		NO_O_ID : oid,
		NO_D_ID : did,
		NO_W_ID : wid,
	}
	gkey := x.gkey()
	found := readtbl(txn, gkey, x)
	return x, found
}

/**
 * Table mutator methods.
 */
func InsertNewOrder(txn *txn.Txn, oid uint32, did uint8, wid uint8) {
	x := &NewOrder {
		NO_O_ID : oid,
		NO_D_ID : did,
		NO_W_ID : wid,
	}
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

func DeleteNewOrder(txn *txn.Txn, oid uint32, did uint8, wid uint8) {
	x := &NewOrder {
		NO_O_ID : oid,
		NO_D_ID : did,
		NO_W_ID : wid,
	}
	gkey := x.gkey()
	deletetbl(txn, gkey)
}

/**
 * Convert primary keys of table NewOrder to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (x *NewOrder) gkey() uint64 {
	var gkey uint64 = uint64(x.NO_D_ID)
	gkey = gkey << 8 + uint64(x.NO_W_ID)
	gkey += TBLID_NEWORDER
	return gkey
}

/**
 * Encode a NewOrder record to an opaque string.
 * Used by TableWrite.
 */
func (x *NewOrder) encode() string {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Encode error: ", err)
	}
	return buf.String()
}

/**
 * Decode an opaque string to a NewOrder record.
 * Used by TableRead.
 */
func (x *NewOrder) decode(opaque string) {
	err := binary.Read(strings.NewReader(opaque), binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Decode error: ", err)
	}
}
