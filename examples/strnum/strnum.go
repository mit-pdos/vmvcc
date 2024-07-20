// / This package converts between strings and numbers. Currently Goose has
// / limited support for manipulating string, so this package is trusted.
package strnum

import (
	"github.com/tchajed/goose/machine"
)

func StringToU64(s string) uint64 {
	return machine.UInt64Get([]byte(s))
}

func U64ToString(n uint64) string {
	buf := make([]byte, 8)
	machine.UInt64Put(buf, n)
	return string(buf)
}
