package trusted_proph

import (
	"github.com/mit-pdos/vmvcc/wrbuf"
	"github.com/tchajed/goose/machine"
)

type ProphId = machine.ProphId

func NewProph() ProphId {
	return machine.NewProph()
}

func ResolveRead(p ProphId, tid uint64, key uint64)           {}
func ResolveAbort(p ProphId, tid uint64)                      {}
func ResolveCommit(p ProphId, tid uint64, wrbuf *wrbuf.WrBuf) {}
