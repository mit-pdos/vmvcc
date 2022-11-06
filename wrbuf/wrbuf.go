package wrbuf

import (
	"github.com/mit-pdos/go-mvcc/common"
	"github.com/mit-pdos/go-mvcc/tuple"
	"github.com/mit-pdos/go-mvcc/index"
)

type WrEnt struct {
	key uint64
	val uint64
	wr  bool
	tpl *tuple.Tuple
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

func (wrbuf *WrBuf) Lookup(key uint64) (uint64, bool, bool) {
	pos, found := search(wrbuf.ents, key)
	if found {
		ent := wrbuf.ents[pos]
		return ent.val, ent.wr, true
	}

	return 0, false, false
}

func (wrbuf *WrBuf) Put(key, val uint64) {
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

func (wrbuf *WrBuf) OpenTuples(tid uint64, idx *index.Index) bool {
	/* Sort entries by key to prevent deadlock. */
	wrbuf.sortEntsByKey()

	/* Start acquiring locks for each key. */
	ents := wrbuf.ents
	var pos uint64 = 0
	for pos < uint64(len(ents)) {
		ent := ents[pos]
		tpl := idx.GetTuple(ent.key)
		ret := tpl.Own(tid)
		if ret != common.RET_SUCCESS {
			/* TODO: can retry a few times for RET_RETRY. */
			break
		}
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
	wrbuf.ents = wrbuf.ents[ : 0]
}
