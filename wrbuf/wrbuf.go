package wrbuf

type WrEnt struct {
	key uint64
	val uint64
	del bool
}

func (ent WrEnt) Key() uint64 {
	return ent.key
}

func (ent WrEnt) Destruct() (uint64, uint64, bool) {
	return ent.key, ent.val, ent.del
}

func search(ents []WrEnt, key uint64) (uint64, bool) {
	var pos uint64 = 0
	for {
		if pos >= uint64(len(ents)) {
			break
		}
		if key == ents[pos].key {
			break
		}
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
		return ent.val, ent.del, true
	}

	return 0, false, false
}

func (wrbuf *WrBuf) Put(key, val uint64) {
	pos, found := search(wrbuf.ents, key)
	if found {
		ent := &wrbuf.ents[pos]
		ent.val = val
		ent.del = false
		return
	}

	ent := WrEnt {
		key : key,
		val : val,
		del : false,
	}
	wrbuf.ents = append(wrbuf.ents, ent)
}

func (wrbuf *WrBuf) Delete(key uint64) {
	pos, found := search(wrbuf.ents, key)
	if found {
		ent := &wrbuf.ents[pos]
		ent.del = true
		return
	}

	ent := WrEnt {
		key : key,
		del : true,
	}
	wrbuf.ents = append(wrbuf.ents, ent)
}

func (wrbuf *WrBuf) IntoEnts() []WrEnt {
	return wrbuf.ents
}

func (wrbuf *WrBuf) Clear() {
	wrbuf.ents = wrbuf.ents[ : 0]
}
