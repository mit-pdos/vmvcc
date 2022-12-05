package tpcc

import (
	"strings"
	"encoding/binary"
	"log"
	"github.com/mit-pdos/go-mvcc/txn"
)

func GetStock(txn *txn.Txn, iid uint32, wid uint8) (*Stock, bool) {
	x := &Stock {
		S_I_ID : iid,
		S_W_ID : wid,
	}
	gkey := x.gkey()
	found := readtbl(txn, gkey, x)
	return x, found
}

func InsertStock(
	txn *txn.Txn,
	iid uint32, wid uint8,
	quantity uint16, ytd uint32,
	ordercnt, remotecnt uint16, data string,
) {
	x := &Stock {
		S_I_ID       : iid,
		S_W_ID       : wid,
		S_QUANTITY   : quantity,
		S_YTD        : ytd,
		S_ORDER_CNT  : ordercnt,
		S_REMOTE_CNT : remotecnt,
	}
	copy(x.S_DATA[:], data)
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

func (x *Stock) Update(
	txn *txn.Txn,
	quantity uint16, ytd uint32, ordercnt uint16, remotecnt uint16,
) {
	x.S_QUANTITY   = quantity
	x.S_YTD        = ytd
	x.S_ORDER_CNT  = ordercnt
	x.S_REMOTE_CNT = remotecnt
	gkey := x.gkey()
	writetbl(txn, gkey, x)
}

/**
 * Convert primary keys of table Stock to a global key.
 * Used by all both TableRead and TableWrite.
 */
func (x *Stock) gkey() uint64 {
	var gkey uint64 = uint64(x.S_I_ID)
	gkey = gkey << 8 + uint64(x.S_W_ID)
	gkey += TBLID_STOCK
	return gkey
}

/**
 * Encode a Stock record to an opaque string.
 * Used by TableWrite.
 */
func (x *Stock) encode() string {
	buf := new(strings.Builder)
	buf.Grow(int(X_S_LEN))
	err := binary.Write(buf, binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Encode error: ", err)
	}
	return buf.String()
}

/**
 * Decode an opaque string to a Stock record.
 * Used by TableRead.
 */
func (x *Stock) decode(opaque string) {
	err := binary.Read(strings.NewReader(opaque), binary.LittleEndian, x)
	if err != nil {
		log.Fatal("Decode error: ", err)
	}
}
