package wrbuf

//  import (
//  	"github.com/mit-pdos/go-mvcc/tuple"
//  	"github.com/mit-pdos/go-mvcc/index"
//  )

type WrEnt struct {
	key uint64
	val uint64
	wr  bool
	// tpl *tuple.Tuple
}

func (ent WrEnt) Key() uint64 {
	return ent.key
}

func (ent WrEnt) Destruct() (uint64, uint64, bool) {
	return ent.key, ent.val, ent.wr
}

func search(ents []WrEnt, key uint64) (uint64, bool) {
	var pos uint64 = 0
	for pos < uint64(len(ents)) && key != ents[pos].key {
		pos++
	}

	found := pos < uint64(len(ents))
	return pos, found
}

type WrBuf struct {
	ents []WrEnt
}

func MkWrBuf() *WrBuf {
	wrbuf := new(WrBuf)
	wrbuf.ents = make([]WrEnt, 0, 16)
	return wrbuf
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

// func (wrbuf *WrBuf) OpenTuples(idx *index.Index) bool {
// 	// TODO: sort keys in ascending order
// 	var ok bool = true
// 	for _, ent := range ents {
// 		tuple := idx.GetTuple(ent.key)
// 		ret := tuple.Own(txn.tid)
// 		if ret != common.RET_SUCCESS {
// 			/* TODO: can retry a few times for RET_RETRY. */
// 			ok = false
// 		}
// 	}
// 
// 	if ok {
// 		return true
// 	}
// 
// 	
// }
// 
// func (wrbuf *WrBuf) UpdateTuples() {
// 	for _, ent := range ents {
// 		key, val, del := ent.Destruct()
// 		idx := txn.idx
// 		tuple := idx.GetTuple(key)
// 		if del {
// 			tuple.KillVersion(txn.tid)
// 		} else {
// 			tuple.AppendVersion(txn.tid, val)
// 		}
// 	}
// }

func (wrbuf *WrBuf) IntoEnts() []WrEnt {
	return wrbuf.ents
}

func (wrbuf *WrBuf) Clear() {
	wrbuf.ents = wrbuf.ents[ : 0]
}
