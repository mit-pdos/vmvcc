package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"log"
	"github.com/mit-pdos/go-mvcc/txn"
)

func GetOrder(txn *txn.Txn, oid uint32, did uint8, wid uint8) (*Order, bool) {
	x := &Order {
		O_ID   : oid,
		O_D_ID : did,
		O_W_ID : wid,
	}
	gkey := x.gkey()
	found := readtbl(txn, gkey, x)
	return x, found
}

func GetOrdersByIndex(
	txn *txn.Txn,
	cid uint32, did uint8, wid uint8,
) []*Order {
	records := make([]*Order, 0, 10)

	/* Read the index entry. */
	x := &Order {
		O_C_ID : cid,
		O_D_ID : did,
		O_W_ID : wid,
	}
	gkeyidx := x.gkeyidx()
	gkeys, found := readidx(txn, gkeyidx)
	if !found {
		return records
	}

	/* Read all the records. */
	for _, gkey := range gkeys {
		r := new(Order)
		readtbl(txn, gkey, r)
		records = append(records, r)
	}

	return records
}

/**
 * Table mutator methods.
 */
func InsertOrder(
	txn *txn.Txn,
	oid uint32, did uint8, wid uint8,
	cid uint32, oentryd uint32, ocarrierid uint8, olcnt uint8, alllocal bool,
) {
	x := &Order {
		O_ID         : oid,
		O_D_ID       : did,
		O_W_ID       : wid,
		O_C_ID       : cid,
		O_ENTRY_D    : oentryd,
		O_CARRIER_ID : ocarrierid,
		O_OL_CNT     : olcnt,
		O_ALL_LOCAL  : alllocal,
	}
	gkey := x.gkey()
	writetbl(txn, gkey, x)

	/* Update index. */
	gkeyidx := x.gkeyidx()
	ents, found := readidx(txn, gkeyidx)
	if !found {
		ents = make([]uint64, 0)
	}
	ents = append(ents, gkey)
	writeidx(txn, gkeyidx, ents)
}

func (x *Order) UpdateCarrier(txn *txn.Txn, ocarrierid uint8) {
	x.O_CARRIER_ID = ocarrierid
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

/**
 * Convert primary keys of table Order to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (x *Order) gkey() uint64 {
	var gkey uint64 = uint64(x.O_ID)
	gkey = gkey << 8 + uint64(x.O_D_ID)
	gkey = gkey << 8 + uint64(x.O_W_ID)
	gkey += TBLID_ORDER
	return gkey
}

func (x *Order) gkeyidx() uint64 {
	var gkey uint64 = uint64(x.O_C_ID)
	gkey = gkey << 8 + uint64(x.O_D_ID)
	gkey = gkey << 8 + uint64(x.O_W_ID)
	gkey += IDXID_ORDER
	return gkey
}

/**
 * Encode a Order record to an opaque string.
 * Used by TableWrite.
 */
func (x *Order) encode() string {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Encode error: ", err)
	}
	return buf.String()
}

/**
 * Decode an opaque string to a Order record.
 * Used by TableRead.
 */
func (x *Order) decode(opaque string) {
	err := binary.Read(strings.NewReader(opaque), binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Decode error: ", err)
	}
}
