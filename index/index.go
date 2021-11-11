package index

import (
	"sync"
	"go-mvcc/tuple"
)

const (
	N_IDX_BUCKET uint64 = 32
)

type IndexBucket struct {
	latch	*sync.Mutex
	m		map[uint64]*tuple.Tuple
}

type Index struct {
	buckets	[]IndexBucket
}

func MkIndex() *Index {
	idx := new(Index)
	idx.buckets = make([]IndexBucket, N_IDX_BUCKET)
	for i := uint64(0); i < N_IDX_BUCKET; i++ {
		b := &idx.buckets[i]
		b.latch = new(sync.Mutex)
		b.m = make(map[uint64]*tuple.Tuple)
		/*
		Rejected by Goose:
		idx.buckets[i].latch = new(sync.Mutex)
		idx.buckets[i].m = make(map[uint64]*tuple.Tuple)
		*/
	}
	return idx
}

func getBucket(key uint64) uint64 {
	return key % N_IDX_BUCKET
}

/**
 * Note that `GetTuple` will always create a tuple when there is no entry in
 * `m`. This design choice seems to be wasting memory resource as we'll always
 * allocate a `Tuple` even with an empty `txn.Get`, but is actually a must: A
 * `txn.Get` should prevent earlier txns from creating new versions, even when
 * it fails to retrieve a value. More specifically, this requirement is enforced
 * with the `tidrd` field of each tuple.
 */
func (idx *Index) GetTuple(key uint64) *tuple.Tuple {
	b := getBucket(key)
	bucket := idx.buckets[b]
	bucket.latch.Lock()

	/* Return the tuple if there exists one. */
	tupleCur, ok := bucket.m[key]
	if ok {
		bucket.latch.Unlock()
		return tupleCur
	}

	/* Create a new tuple and associate it with the key. */
	tupleNew := tuple.MkTuple()
	bucket.m[key] = tupleNew

	bucket.latch.Unlock()
	return tupleNew
}

func (idx *Index) GetKeys() []uint64 {
	/* TODO: Try to estimate initial cap. */
	var keys []uint64
	keys = make([]uint64, 0, 2000)
	for b := uint64(0); b < N_IDX_BUCKET; b++ {
		bucket := idx.buckets[b]
		bucket.latch.Lock()
		for k := range bucket.m {
			keys = append(keys, k)
		}
		bucket.latch.Unlock()
	}
	return keys
}

