package main

import (
	"github.com/mit-pdos/vmvcc/vmvcc"
	"unsafe"
)

func readidx(txn *vmvcc.Txn, gkey uint64) ([]uint64, bool) {
	opaque, found := txn.Read(gkey)
	if !found {
		return nil, false
	}
	ents := decodeidx(opaque)
	return ents, true
}

func writeidx(txn *vmvcc.Txn, gkey uint64, ents []uint64) {
	s := encodeidx(ents)
	txn.Write(gkey, s)
}

/**
 * Encode a slice of global keys pointing to table records to an opaque string.
 */
func encodeidx(gkeys []uint64) string {
	n := uint64(len(gkeys))
	buf := make([]byte, n*8+8)
	encodeU64(buf, n, 0)
	copy(buf[8:], unsafe.Slice((*byte)(unsafe.Pointer(&gkeys[0])), n*8))
	return string(buf)
}

/**
 * Decode an opaque string to a slice of global keys pointing to table records.
 */
func decodeidx(opaque string) []uint64 {
	var n uint64
	decodeU64(&n, opaque, 0)
	gkeys := make([]uint64, n)
	copy(unsafe.Slice((*byte)(unsafe.Pointer(&gkeys[0])), n*8), opaque[8:])
	return gkeys
}
