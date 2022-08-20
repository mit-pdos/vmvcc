package tid

import (
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/go-mvcc/config"
)

func GenTID(sid uint64) uint64 {
	var tid uint64

	/* Call `GetTSC` and round the result up to site ID boundary. */
	tid = grove_ffi.GetTSC()
	tid = ((tid + config.N_TXN_SITES) & ^(config.N_TXN_SITES - 1)) + sid
	// Below is the old (and wrong) version where we simply round the result,
	// up or down, to site ID boundary.
	// tid = (tid & ^(config.N_TXN_SITES - 1)) + sid

	/* Wait until TSC exceeds TID. */
	for grove_ffi.GetTSC() <= tid {
	}

	return tid
}

