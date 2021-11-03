package gc

import (
	"go-mvcc/index"
)

type GC struct {
	idx		*index.Index
	/* Add some statistics to profile GC efficiency. */
}

func MkGC(idx *index.Index) *GC {
	gc := new(GC)
	gc.idx = idx
	return gc
}

func (gc *GC) Start(tidMin uint64) {
	idx := gc.idx
	keys := idx.GetKeys()
	for _, k := range keys {
		tuple := idx.GetTuple(k)
		tuple.RemoveVersions(tidMin)
	}
}

