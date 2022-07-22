package proph

import (
	"github.com/mit-pdos/go-mvcc/wrbuf"
	"github.com/tchajed/goose/machine"
)

func ResolveRead(p machine.ProphId, tid uint64, key uint64) {}
func ResolveAbort(p machine.ProphId, tid uint64) {}
func ResolveCommit(p machine.ProphId, tid uint64, wrbuf *wrbuf.WrBuf) {}
