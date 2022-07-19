// +build !goose
package proph

import (
	"github.com/mit-pdos/go-mvcc/txn"
	"github.com/tchajed/goose/machine"
)

func ResolveRead(p machine.ProphId, tid uint64, key uint64) {}
func ResolveAbort(p machine.ProphId, tid uint64) {}

// Resolves only the 'key' and 'val' of the WrEnt, not the 'tuple'.
func ResolveCommit(p machine.ProphId, tid uint64, wset []txn.WrEnt) {}
