package index

import (
	"sync"
	"go-mvcc/tuple"
)

/**
 * TODO: Use lock-striped hash map.
 */
type Index struct {
	latch	*sync.Mutex
	m		map[uint64]*tuple.Tuple
}

/**
 * Postconditions:
 * 1. `create == true` -> return true
 */
func (idx *Index) GetTuple(key uint64, create bool) (*tuple.Tuple, bool) {
	idx.latch.Lock()

	/* Return the tuple if there exists one. */
	tupleCur, ok := idx.m[key]
	if ok {
		idx.latch.Unlock()
		return tupleCur, true
	}

	/* Return `false` if not exsits AND we don't want to create one. */
	if !create {
		idx.latch.Unlock()
		return nil, false
	}

	/* Create a new tuple and associate it with the key. */
	tupleNew := tuple.MkTuple()
	idx.m[key] = tupleNew
	idx.latch.Unlock()
	return tupleNew, true
}

