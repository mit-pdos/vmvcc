package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
	"github.com/mit-pdos/go-mvcc/txn"
)

func GetNewOrder(txn *txn.Txn, did uint8, wid uint8) *NewOrder {
	x := &NewOrder {
		NO_D_ID : did,
		NO_W_ID : wid,
	}
	gkey := x.gkey()
	readtbl(txn, gkey, x)
	return x
}

/**
 * Table mutator methods.
 */
func InsertNewOrder(txn *txn.Txn, did uint8, wid uint8, oid uint32) {
	x := &NewOrder {
		NO_D_ID : did,
		NO_W_ID : wid,
	}
	gkey := x.gkey()
	found := readtbl(txn, gkey, x)
	if !found {
		x.NO_O_IDS = make([]uint32, 0)
	}

	/* Check if `oid` does not alreay exist. */
	for _, oidx := range x.NO_O_IDS {
		if oidx == oid {
			return
		}
	}

	x.NO_O_IDS = append(x.NO_O_IDS, oid)
	writetbl(txn, gkey, x)
}

func DeleteNewOrder(txn *txn.Txn, did uint8, wid uint8, oid uint32) {
	x := &NewOrder {
		NO_D_ID : did,
		NO_W_ID : wid,
	}
	gkey := x.gkey()
	found := readtbl(txn, gkey, x)
	if !found {
		return
	}

	/* Check if `oid` does not alreay exist. */
	var idx int
	for idx = 0; idx < len(x.NO_O_IDS); idx++ {
		if x.NO_O_IDS[idx] == oid {
			break
		}
	}
	if idx == len(x.NO_O_IDS) {
		return
	}

	x.NO_O_IDS = append(x.NO_O_IDS[: idx], x.NO_O_IDS[idx + 1 :]...)
	writetbl(txn, gkey, x)
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
