package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
	"github.com/mit-pdos/go-mvcc/txn"
)

func GetItem(txn *txn.Txn, iid uint32) (*Item, bool) {
	x := &Item { I_ID : iid }
	gkey := x.gkey()
	found := readtbl(txn, gkey, x)
	return x, found
}

/**
 * Table mutator methods.
 */
func InsertItem(
	txn *txn.Txn,
	iid uint32,
	imid uint32, name string, price float32, data string,
) {
	x := &Item {
		I_ID    : iid,
		I_IM_ID : imid,
		I_PRICE : price,
	}
	copy(x.I_NAME[:], name)
	copy(x.I_DATA[:], data)
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

/**
 * Convert primary keys of table Item to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (x *Item) gkey() uint64 {
	var gkey uint64 = uint64(x.I_ID)
	gkey += TBLID_ITEM
	return gkey
}

/**
 * Encode a Item record to an opaque string.
 * Used by TableWrite.
 */
func (x *Item) encode() string {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Encode error: ", err)
	}
	return buf.String()
}

/**
 * Decode an opaque string to a Item record.
 * Used by TableRead.
 */
func (x *Item) decode(opaque string) {
	err := binary.Read(strings.NewReader(opaque), binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Decode error: ", err)
	}
}
