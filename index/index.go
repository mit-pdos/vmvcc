package index

import (
	"sync"
	"github.com/mit-pdos/vmvcc/config"
	"github.com/mit-pdos/vmvcc/tuple"
)

type IndexBucket struct {
	latch	*sync.Mutex
	m		map[uint64]*tuple.Tuple
}

type Index struct {
	buckets	[]*IndexBucket
}

func MkIndex() *Index {
	idx := new(Index)
	idx.buckets = make([]*IndexBucket, config.N_IDX_BUCKET)
	for i := uint64(0); i < config.N_IDX_BUCKET; i++ {
		b := new(IndexBucket)
		b.latch = new(sync.Mutex)
		b.m = make(map[uint64]*tuple.Tuple)
		idx.buckets[i] = b
	}
	return idx
}

func getBucket(key uint64) uint64 {
	return (key >> 52 + key) % config.N_IDX_BUCKET
	// return key % config.N_IDX_BUCKET
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

func (idx *Index) getKeys() []uint64 {
	var keys []uint64
	/* TODO: Try a better initial cap by summing up each bucket. */
	keys = make([]uint64, 0, 200)
	// for b := uint64(0); b < config.N_IDX_BUCKET; b++ {
	for _, bkt := range idx.buckets {
		bkt.latch.Lock()
		for k := range bkt.m {
			keys = append(keys, k)
		}
		bkt.latch.Unlock()
	}
	return keys
}

func (idx *Index) DoGC(tidMin uint64) {
	keys := idx.getKeys()
	for _, k := range keys {
		tuple := idx.GetTuple(k)
		tuple.RemoveVersions(tidMin)
	}
}

