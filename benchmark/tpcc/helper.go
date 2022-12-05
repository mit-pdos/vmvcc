package tpcc

import (
	"bytes"
	"unsafe"
	"math/rand"
)

func beforeNull(b []byte) []byte {
	/* Find the index of the first null character. Assembly in Go. */
	i := bytes.IndexByte(b, 0)

	if i == -1 {
		/* Return `b` if no null character is found in `b`. */
		return b
	} else {
		return b[: i]
	}
}

/* Both `pickBetween` and `pickBetweenNonUniformly` are inclusive. */
func pickBetween(rd *rand.Rand, min, max uint32) uint32 {
	n := rd.Uint32() % (max - min + 1) + min
	return n
}

func pickBetweenNonUniformly(rd *rand.Rand, a, c, min, max uint32) uint32 {
	/* See Silo tpcc.cc:L360. */
	x := (pickBetween(rd, 0, a) | pickBetween(rd, min, max)) + c
	y := x % (max - min + 1) + min
	return y
}

func trueWithProb(rd *rand.Rand, prob uint32) bool {
	b := rd.Uint32() % 100 < prob
	return b
}

/**
 * Assume:
 * 1. nwh > 0
 * 2. wid > 0
 * 3. wid <= nwh
 */
func pickWarehouseIdExcept(rd *rand.Rand, nwh, wid uint8) uint8 {
	if nwh == 1 {
		return 1
	}

	widret := wid
	for widret == wid {
		widret = uint8(rd.Uint32() % uint32(nwh)) + 1
	}

	return widret
}

func pickNOrderLines(rd *rand.Rand) uint8 {
	n := uint8(pickBetween(rd, uint32(OL_MIN_CNT), uint32(OL_MAX_CNT)))
	return n
}

func pickQuantity(rd *rand.Rand) uint8 {
	n := uint8(pickBetween(rd, 1, uint32(OL_MAX_QUANTITY)))
	return n
}

/* TODO */
func getTime() uint32 {
	return 0
}

func encodeU8(buf []byte, n uint8, offset uint64) {
	buf[offset] = n
}

func encodeU16(buf []byte, n uint16, offset uint64) {
	//binary.LittleEndian.PutUint16(buf[offset :], n)
	copy(buf[offset :], unsafe.Slice((*byte)(unsafe.Pointer(&n)), 2))
}

func encodeU32(buf []byte, n uint32, offset uint64) {
	//binary.LittleEndian.PutUint32(buf[offset :], n)
	copy(buf[offset :], unsafe.Slice((*byte)(unsafe.Pointer(&n)), 4))
}

func encodeU64(buf []byte, n uint64, offset uint64) {
	//binary.LittleEndian.PutUint64(buf[offset :], n)
	copy(buf[offset :], unsafe.Slice((*byte)(unsafe.Pointer(&n)), 8))
}

func encodeF32(buf []byte, n float32, offset uint64) {
	copy(buf[offset :], unsafe.Slice((*byte)(unsafe.Pointer(&n)), 4))
}

func encodeBytes(buf []byte, src []byte, offset uint64) {
	copy(buf[offset :], src)
}

func decodeU8(ptr *uint8, s string, offset uint64) {
	copy(unsafe.Slice((*byte)(unsafe.Pointer(ptr)), 1), s[offset :])
}

func decodeU16(ptr *uint16, s string, offset uint64) {
	copy(unsafe.Slice((*byte)(unsafe.Pointer(ptr)), 2), s[offset :])
}

func decodeU32(ptr *uint32, s string, offset uint64) {
	copy(unsafe.Slice((*byte)(unsafe.Pointer(ptr)), 4), s[offset :])
}

func decodeU64(ptr *uint64, s string, offset uint64) {
	copy(unsafe.Slice((*byte)(unsafe.Pointer(ptr)), 8), s[offset :])
}

func decodeString(buf []byte, s string, offset uint64) {
	copy(buf, s[offset :])
}

func decodeF32(ptr *float32, s string, offset uint64) {
	copy(unsafe.Slice((*byte)(unsafe.Pointer(ptr)), 4), s[offset :])
}
