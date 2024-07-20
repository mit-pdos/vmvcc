package wrbuf

import (
	"github.com/mit-pdos/vmvcc/common"
	"github.com/mit-pdos/vmvcc/index"
	"github.com/mit-pdos/vmvcc/tuple"
)

// @key and @val
// Key-value pair of the write entry.
//
// @wr
// Write @key with @val, or delete @key.
//
// @tpl
// Tuple pointer. Exists to save one index-lookup per write entry--written by
// @OpenTuples and read by @UpdateTuples.
type WrEnt struct {
	key uint64
	val string
	wr  bool
	tpl *tuple.Tuple
}

// Linear search can be quite slow when there are many entries.
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
			if ents[j-1].key <= ents[j].key {
				break
			}
			swap(ents, j-1, j)
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
		ent.wr = true
		return
	}

	ent := WrEnt{
		key: key,
		val: val,
		wr:  true,
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

	ent := WrEnt{
		key: key,
		wr:  false,
	}
	wrbuf.ents = append(wrbuf.ents, ent)
}

// @OpenTuples acquires the write locks of tuples to be updated by calling
// @tuple.Own.
//
// This design prevents deadlocks that show up in the design where transactions
// acquire write locks on writes. For instance, consider the following
// scenarios:
//
// Txn A | W(x)    R(y)
// Txn B |     W(y)    R(x)
//
// This causes a deadlock because A's R(y) waits on B's W(y), and B's R(x) waits
// on A's W(x). Acquring write locks at commit time breaks the wait-cycle since
// if one transaction waits on another, this means the latter must have entered
// the commit phase, and therefore never waits on anything.
func (wrbuf *WrBuf) OpenTuples(tid uint64, idx *index.Index) bool {
	// Mistakenly think we need to sort the entries by keys to prevent
	// deadlocks, but keeping it here since empirically it slightly improves
	// performance for some reasons.
	wrbuf.sortEntsByKey()

	// Start acquiring locks for each key.
	ents := wrbuf.ents
	var pos uint64 = 0
	for pos < uint64(len(ents)) {
		ent := ents[pos]
		tpl := idx.GetTuple(ent.key)
		ret := tpl.Own(tid)
		if ret != common.RET_SUCCESS {
			// TODO: can retry a few times for RET_RETRY.
			break
		}
		// A more efficient way is updating field @tpl, but not
		// supported by Goose.
		ents[pos] = WrEnt{
			key: ent.key,
			val: ent.val,
			wr:  ent.wr,
			tpl: tpl,
		}
		pos++
	}

	// Release partially acquired locks.
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
		ent.tpl.WriteOpen()
	}
	return true
}

func (wrbuf *WrBuf) UpdateTuples(tid uint64) {
	ents := wrbuf.ents
	for _, ent := range ents {
		tpl := ent.tpl
		if ent.wr {
			tpl.AppendVersion(tid, ent.val)
		} else {
			tpl.KillVersion(tid)
		}
	}
}

func (wrbuf *WrBuf) Clear() {
	wrbuf.ents = wrbuf.ents[:0]
}
