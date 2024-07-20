package trusted_proph

import (
	"github.com/goose-lang/primitive"
	"github.com/mit-pdos/vmvcc/wrbuf"
)

type ProphId = primitive.ProphId

func NewProph() ProphId {
	return primitive.NewProph()
}

func ResolveRead(p ProphId, tid uint64, key uint64)           {}
func ResolveAbort(p ProphId, tid uint64)                      {}
func ResolveCommit(p ProphId, tid uint64, wrbuf *wrbuf.WrBuf) {}
