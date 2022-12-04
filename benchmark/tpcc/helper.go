package tpcc

import (
	"bytes"
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
