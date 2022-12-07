package tplock

import (
	//"time"
	"sync"
	"github.com/mit-pdos/go-mvcc/config"
	// "github.com/mit-pdos/go-mvcc/cfmutex"
)


/**
 * Tuple.
 */
type Tuple struct {
	latch *sync.Mutex
	rcond *sync.Cond
	lock  uint32
	del   bool
	val   string
}

func MkTuple() *Tuple {
	tuple := new(Tuple)
	tuple.latch = new(sync.Mutex)
	tuple.rcond = sync.NewCond(tuple.latch)
	tuple.lock = 0
	tuple.del = true

	return tuple
}

func (tuple *Tuple) Own(ownrd bool) bool {
	var ok bool

	tuple.latch.Lock()

	if tuple.lock == 0 || (tuple.lock == 1 && ownrd) {
		ok = true
		tuple.lock = 0xffffffff
	} else {
		ok = false
	}
	tuple.latch.Unlock()

	return ok
}

func (tuple *Tuple) WriteLock() {
	tuple.latch.Lock()
}

func (tuple *Tuple) Write(val string) {
	tuple.val = val
	tuple.del = false
	tuple.lock = 0
	tuple.rcond.Broadcast()

	tuple.latch.Unlock()
}

func (tuple *Tuple) Kill() {
	tuple.del = true
	tuple.lock = 0
	tuple.rcond.Broadcast()

	tuple.latch.Unlock()
}

func (tuple *Tuple) Free() {
	tuple.latch.Lock()

	tuple.lock = 0
	tuple.rcond.Broadcast()

	tuple.latch.Unlock()
}

func (tuple *Tuple) Read(first bool) (string, bool) {
	tuple.latch.Lock()

	for tuple.lock == 0xffffffff {
		tuple.rcond.Wait()
	}

	if first {
		tuple.lock++
	}

	tuple.latch.Unlock()
	return tuple.val, !tuple.del
}

func (tuple *Tuple) ReadRelease() {
	tuple.latch.Lock()

	tuple.lock--

	tuple.latch.Unlock()
}

/**
 * Write buffer.
 */
type WrEnt struct {
	key uint64
	val string
	wr  bool
	tpl *Tuple
}

func search(ents []WrEnt, key uint64) (uint64, bool) {
	var pos uint64 = 0
	for pos < uint64(len(ents)) && key != ents[pos].key {
		pos++
	}

	found := pos < uint64(len(ents))
	return pos, found
}

func swap(ents []WrEnt, i, j uint64) {
	tmp := ents[i]
	ents[i] = ents[j]
	ents[j] = tmp
}

type WrBuf struct {
	ents []WrEnt
}

func MkWrBuf() *WrBuf {
	wrbuf := new(WrBuf)
	wrbuf.ents = make([]WrEnt, 0, 16)
	return wrbuf
}

func (wrbuf *WrBuf) sortEntsByKey() {
	ents := wrbuf.ents
	var i uint64 = 1
	for i < uint64(len(ents)) {
		var j uint64 = i
		for j > 0 {
			if ents[j - 1].key <= ents[j].key {
				break
			}
			swap(ents, j - 1, j)
			j--
		}
		i++
	}
}

func (wrbuf *WrBuf) Lookup(key uint64) (string, bool, bool) {
	pos, found := search(wrbuf.ents, key)
	if found {
		ent := wrbuf.ents[pos]
		return ent.val, ent.wr, true
	}

	return "", false, false
}

func (wrbuf *WrBuf) Put(key uint64, val string) {
	pos, found := search(wrbuf.ents, key)
	if found {
		ent := &wrbuf.ents[pos]
		ent.val = val
		ent.wr  = true
		return
	}

	ent := WrEnt {
		key : key,
		val : val,
		wr  : true,
	}
	wrbuf.ents = append(wrbuf.ents, ent)
}

func (wrbuf *WrBuf) Delete(key uint64) {
	pos, found := search(wrbuf.ents, key)
	if found {
		ent := &wrbuf.ents[pos]
		ent.wr = false
		return
	}

	ent := WrEnt {
		key : key,
		wr  : false,
	}
	wrbuf.ents = append(wrbuf.ents, ent)
}

func (wrbuf *WrBuf) OpenTuples(idx *Index, rdset map[uint64]struct{}) bool {
	/* Sort entries by key to prevent deadlock. */
	wrbuf.sortEntsByKey()

	/* Start acquiring locks for each key. */
	ents := wrbuf.ents
	var pos uint64 = 0
	for pos < uint64(len(ents)) {
		ent := ents[pos]
		tpl := idx.GetTuple(ent.key)
		_, found := rdset[ent.key]
		ret := tpl.Own(found)
		if !ret {
			break
		}
		/* Escalte the read lock to write lock. */
		delete(rdset, ent.key)
		// A more efficient way is updating field `tpl`, but not supported by Goose.
		ents[pos] = WrEnt {
			key : ent.key,
			val : ent.val,
			wr  : ent.wr,
			tpl : tpl,
		}
		pos++
	}

	/* Release partially acquired locks. */
	if pos < uint64(len(ents)) {
		var i uint64 = 0
		for i < pos {
			tpl := ents[i].tpl
			tpl.Free()
			i++
		}
		return false
	}

	for _, ent := range ents {
		ent.tpl.WriteLock()
	}
	return true
}

func (wrbuf *WrBuf) UpdateTuples() {
	ents := wrbuf.ents
	for _, ent := range ents {
		tpl := ent.tpl
		if ent.wr {
			tpl.Write(ent.val)
		} else {
			tpl.Kill()
		}
	}
}

func (wrbuf *WrBuf) Clear() {
	wrbuf.ents = wrbuf.ents[ : 0]
}


/**
 * Index.
 */
type IndexBucket struct {
	latch *sync.Mutex
	m     map[uint64]*Tuple
}

type Index struct {
	buckets []*IndexBucket
}

func MkIndex() *Index {
	idx := new(Index)
	idx.buckets = make([]*IndexBucket, config.N_IDX_BUCKET)
	for i := uint64(0); i < config.N_IDX_BUCKET; i++ {
		b := new(IndexBucket)
		b.latch = new(sync.Mutex)
		b.m = make(map[uint64]*Tuple)
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
 * allocate a `Tuple` even with an empty `txn.Get`, but is actually a must: An
 * empty `txn.Get` should prevent other transactions from inserting a new one
 * during the execution of this transaction.
 */
func (idx *Index) GetTuple(key uint64) *Tuple {
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
	tupleNew := MkTuple()
	bucket.m[key] = tupleNew

	bucket.latch.Unlock()
	return tupleNew
}


/**
 * Transaction.
 */
type Txn struct {
	wrbuf *WrBuf
	rdset map[uint64]struct{}
	idx   *Index
}

type TxnMgr struct {
	idx *Index
}

func MkTxnMgr() *TxnMgr {
	mgr := new(TxnMgr)
	mgr.idx = MkIndex()
	return mgr
}

func (txnMgr *TxnMgr) New() *Txn {
	/* Make a new txn. */
	txn := new(Txn)
	txn.wrbuf = MkWrBuf()
	txn.rdset = make(map[uint64]struct{})
	txn.idx = txnMgr.idx

	return txn
}

func (txnMgr *TxnMgr) ActivateGC() {
	/* Do nothing. Just for compatibility. */
}


func (txn *Txn) Put(key uint64, val string) {
	wrbuf := txn.wrbuf
	wrbuf.Put(key, val)
}

func (txn *Txn) Delete(key uint64) bool {
	wrbuf := txn.wrbuf
	wrbuf.Delete(key)

	return true
}

func (txn *Txn) Get(key uint64) (string, bool) {
	/* First try to find `key` in the local write set. */
	wrbuf := txn.wrbuf
	valb, wr, found := wrbuf.Lookup(key)
	if found {
		return valb, wr
	}

	_, found = txn.rdset[key]

	idx := txn.idx
	tuple := idx.GetTuple(key)
	txn.rdset[key] = struct{}{}
	val, found := tuple.Read(!found)

	return val, found
}

func (txn *Txn) begin() {
	txn.wrbuf.Clear()
	txn.rdset = make(map[uint64]struct{})
}

func (txn *Txn) acquire() bool {
	ok := txn.wrbuf.OpenTuples(txn.idx, txn.rdset)
	return ok
}

func (txn *Txn) releaseReadLocks() {
	for k := range txn.rdset {
		tpl := txn.idx.GetTuple(k)
		tpl.ReadRelease()
	}
}

func (txn *Txn) commit() {
	/* At this point we have all the read and write locks. First release wlocks. */
	txn.wrbuf.UpdateTuples()
	txn.releaseReadLocks()
}

func (txn *Txn) abort() {
	txn.releaseReadLocks()
}

func (txn *Txn) DoTxn(body func(txn *Txn) bool) bool {
	txn.begin()
	cmt := body(txn)
	if !cmt {
		txn.abort()
		return false
	}
	ok := txn.acquire()
	if !ok {
		txn.abort()
		return false
	}
	txn.commit()
	return true
}
