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

type WrBuf struct {
	ents []WrEnt
}

func (wrbuf *WrBuf) lookup(key uint64) (uint64, bool) {
	var pos uint64 = 0
	for {
		if pos >= uint64(len(wrbuf.ents)) {
			break
		}
		if key == wrbuf.ents[pos].key {
			break
		}
		pos++
	}

	found := pos < uint64(len(wrbuf.ents))
	return pos, found
}

func (wrbuf *WrBuf) Lookup(key uint64) (uint64, bool, bool) {
	pos, found := wrbuf.lookup(key)
	if found {
		ent := wrbuf.ents[pos]
		return ent.val, ent.del, true
	}

	return 0, false, false
}

func (wrbuf *WrBuf) Put(key, val uint64) {
	pos, found := wrbuf.lookup(key)
	if found {
		went := &wrbuf.ents[pos]
		went.val = val
		went.del = false
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
	pos, found := wrbuf.lookup(key)
	if found {
		went := &wrbuf.ents[pos]
		went.del = true
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
