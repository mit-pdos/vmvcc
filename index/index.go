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

func MkIndex() *Index {
	idx := new(Index)
	idx.latch = new(sync.Mutex)
	idx.m = make(map[uint64]*tuple.Tuple)
	return idx
}

/**
 * Note that `GetTuple` will always create a tuple when there is no entry in
 * `m`. This design choice seems to be wasting memory resource as we'll allocate
 * a `Tuple` even with a `txn.Get`, but is actually a must: A non-existent
 * `txn.Get` should prevent earlier txns from creating new versions. More
 * specifically, this requirement is enforced with the `tidrd` field of each
 * tuple.
 */
func (idx *Index) GetTuple(key uint64) *tuple.Tuple {
	idx.latch.Lock()

	/* Return the tuple if there exists one. */
	tupleCur, ok := idx.m[key]
	if ok {
		idx.latch.Unlock()
		return tupleCur
	}

	/* Create a new tuple and associate it with the key. */
	tupleNew := tuple.MkTuple()
	idx.m[key] = tupleNew

	idx.latch.Unlock()
	return tupleNew
}

