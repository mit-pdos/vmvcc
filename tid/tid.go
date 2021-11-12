package tid

import (
	"go-mvcc/config"
)

func getTSC() uint64

func GetTID(token uint64) uint64 {
	tsc := getTSC()
	tid := (tsc & ^(config.MAX_TXN - 1)) + token
	return tid
}

