package tpcc

import (
	"bytes"
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
