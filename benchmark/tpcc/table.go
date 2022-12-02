package tpcc

import (
	"bytes"
	"strings"
	"encoding/binary"
	"github.com/mit-pdos/go-mvcc/txn"
)

type Table interface {
	gkey() uint64
	encode() string
	decode(opaque string)
}

type IndexedTable interface {
	Table
	gkeyidx() uint64
}

/**
 * Reader and writer operation invoking transation methods.
 */
func ReadTable(tbl Table, txn *txn.Txn) bool {
	gkey := tbl.gkey()
	opaque, found := txn.Get(gkey)
	if !found {
		return false
	}
	tbl.decode(opaque)
	return true
}

func WriteTable(tbl Table, txn *txn.Txn) {
	gkey := tbl.gkey()
	s := tbl.encode()
	txn.Put(gkey, s)
}

/**
 * Encode a slice of global keys pointing to table records to an opaque string.
 */
func encodeidx(gkeys []uint64) string {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, gkeys)
	return buf.String()
}

/**
 * Decode an opaque string to a slice of global keys pointing to table records.
 */
func decodeidx(opaque string) []uint64 {
	// TODO: preallocate
	gkeys := make([]uint64, 0)
	binary.Read(strings.NewReader(opaque), binary.LittleEndian, &gkeys)
	return gkeys
}

func WriteIndexedTable(tbl IndexedTable, txn *txn.Txn) {
	/* Update the table. */
	gkey := tbl.gkey()
	s := tbl.encode()
	txn.Put(gkey, s)

	/* Read the index entry. */
	gkeyidx := tbl.gkeyidx()
	opaque, found := txn.Get(gkeyidx)
	var gkeys []uint64
	if found {
		gkeys = decodeidx(opaque)
	} else {
		/* Create a fresh slice of gkeys for the new index entry. */
		gkeys = make([]uint64, 0)
	}
	gkeys = append(gkeys, gkey)

	/* Update the index entry. */
	s = encodeidx(gkeys)
	txn.Put(gkeyidx, s)
}

// func readidx(tbl IndexedTable, txn *txn.Txn) []uint64 {
// }
