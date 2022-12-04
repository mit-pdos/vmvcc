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

/**
 * Assume:
 * 1. nwh > 0
 * 2. wid > 0
 * 3. wid <= nwh
 *
 * Could additionally take a `*Rand`.
 */
func pickWarehouseIdExcept(nwh, wid uint8) uint8 {
	if nwh == 1 {
		return 1
	}

	widret := wid
	for widret == wid {
		widret = uint8(rand.Uint32() % uint32(nwh)) + 1
	}

	return widret
}
