package txn

import (
	//"fmt"
	//"time"
	"github.com/mit-pdos/vmvcc/config"
	"github.com/mit-pdos/vmvcc/index"
	"github.com/mit-pdos/vmvcc/wrbuf"
	"github.com/mit-pdos/vmvcc/trusted_proph"
	"github.com/mit-pdos/vmvcc/tid"
	"github.com/mit-pdos/vmvcc/cfmutex"
	"github.com/tchajed/goose/machine"
)

// @tid
// Transaction ID.
//
// @wrbuf
// Write buffer.
//
// @idx
// Pointer to the index.
type Txn struct {
	tid   uint64
	sid   uint64
	wrbuf *wrbuf.WrBuf
	idx   *index.Index
	mgr   *TxnMgr
}

// @tids
// Actives transaction IDs.
type TxnSite struct {
	latch   *cfmutex.CFMutex
	tids    []uint64
	padding [4]uint64
}

// @sid
// Current site ID.
//
// @sites
// Transaction sites.
type TxnMgr struct {
	latch  *cfmutex.CFMutex
	sid    uint64
	sites  []*TxnSite
	idx    *index.Index
	proph  machine.ProphId
}

func MkTxnMgr() *TxnMgr {
	proph := machine.NewProph()
	mgr := &TxnMgr { proph : proph }
	mgr.latch = new(cfmutex.CFMutex)
	mgr.sites = make([]*TxnSite, config.N_TXN_SITES)

	// Call this once for establishing invariants.
	tid.GenTID(0)
	for i := uint64(0); i < config.N_TXN_SITES; i++ {
		site := new(TxnSite)
		site.latch = new(cfmutex.CFMutex)
		site.tids = make([]uint64, 0, 8)
		mgr.sites[i] = site
	}
	mgr.idx = index.MkIndex()

	return mgr
}

func (mgr *TxnMgr) New() *Txn {
	mgr.latch.Lock()

	txn := new(Txn)
	txn.wrbuf = wrbuf.MkWrBuf()
	sid := mgr.sid
	txn.sid = sid
	txn.idx = mgr.idx
	txn.mgr = mgr

	mgr.sid = sid + 1
	if mgr.sid >= config.N_TXN_SITES {
		mgr.sid = 0
	}

	mgr.latch.Unlock()
	return txn
}

// @activate adds @tid to the set of active transaction IDs.
func (mgr *TxnMgr) activate(sid uint64) uint64 {
	site := mgr.sites[sid]
	site.latch.Lock()

	var t uint64
	t = tid.GenTID(sid)
	// Assume TID never overflow.
	machine.Assume(t < 18446744073709551615)

	site.tids = append(site.tids, t)

	site.latch.Unlock()
	return t
}

func findTID(tid uint64, tids []uint64) uint64 {
	// Require @tids contains @tid.
	var idx uint64 = 0
	for tid != tids[idx] {
		idx++
	}

	return idx
}

func swapWithEnd(xs []uint64, i uint64) {
	// Require (1) @xs not empty, (2) @i < len(@xs).
	tmp := xs[len(xs) - 1]
	xs[len(xs) - 1] = xs[i]
	xs[i] = tmp
}

// @deactivate removes @tid from the set of active transaction IDs.
func (mgr *TxnMgr) deactivate(sid uint64, tid uint64) {
	// Require @mgr.tids contains @tid.
	site := mgr.sites[sid]
	site.latch.Lock()

	// Remove @tid from the set of active transactions.
	idx := findTID(tid, site.tids)
	swapWithEnd(site.tids, idx)
	site.tids = site.tids[:len(site.tids) - 1]

	site.latch.Unlock()
}

// @getSafeTS returns a per-site lower bound on the active/future transaction
// IDs.
func (mgr *TxnMgr) getMinActiveTIDSite(sid uint64) uint64 {
	site := mgr.sites[sid]
	site.latch.Lock()

	var tidnew uint64
	tidnew = tid.GenTID(sid)
	machine.Assume(tidnew < 18446744073709551615)

	var tidmin uint64 = tidnew
	for _, tid := range site.tids {
		if tid < tidmin {
			tidmin = tid
		}
	}

	site.latch.Unlock()
	return tidmin
}

// @GetSafeTS returns a lower bound on the active/future transaction IDs.
func (mgr *TxnMgr) getMinActiveTID() uint64 {
	var min uint64 = config.TID_SENTINEL
	for sid := uint64(0); sid < config.N_TXN_SITES; sid++ {
		tid := mgr.getMinActiveTIDSite(sid)
		if tid < min {
			min = tid
		}
	}

	return min
}

// Only used for testing and profiling.
func (mgr *TxnMgr) getNumActiveTxns() uint64 {
	var n uint64 = 0
	for sid := uint64(0); sid < config.N_TXN_SITES; sid++ {
		site := mgr.sites[sid]
		site.latch.Lock()
		n += uint64(len(site.tids))
		site.latch.Unlock()
	}

	return n
}

func (mgr *TxnMgr) gc() {
	tidMin := mgr.getMinActiveTID()
	if tidMin < config.TID_SENTINEL {
		mgr.idx.DoGC(tidMin)
	}
}

func (mgr *TxnMgr) ActivateGC() {
	go func() {
		for {
			mgr.gc()
			machine.Sleep(1 * 1000000)
		}
	}()
}

func (txn *Txn) Put(key uint64, val string) {
	wrbuf := txn.wrbuf
	wrbuf.Put(key, val)
}

func (txn *Txn) Delete(key uint64) bool {
	wrbuf := txn.wrbuf
	wrbuf.Delete(key)

	// To support SQL in the future, @Delete should return false when not found.
	return true
}

func (txn *Txn) Get(key uint64) (string, bool) {
	// First try to find @key in the local write set.
	wrbuf := txn.wrbuf
	valb, wr, found := wrbuf.Lookup(key)
	if found {
		return valb, wr
	}

	idx := txn.idx
	tuple := idx.GetTuple(key)
	tuple.ReadWait(txn.tid)
	trusted_proph.ResolveRead(txn.mgr.proph, txn.tid, key)
	val, found := tuple.ReadVersion(txn.tid)

	return val, found
}

func (txn *Txn) begin() {
	tid := txn.mgr.activate(txn.sid)
	txn.tid = tid
	txn.wrbuf.Clear()
}

func (txn *Txn) acquire() bool {
	ok := txn.wrbuf.OpenTuples(txn.tid, txn.idx)
	return ok
}

func (txn *Txn) commit() {
	trusted_proph.ResolveCommit(txn.mgr.proph, txn.tid, txn.wrbuf)
	txn.wrbuf.UpdateTuples(txn.tid)
	txn.mgr.deactivate(txn.sid, txn.tid)
}

func (txn *Txn) abort() {
	trusted_proph.ResolveAbort(txn.mgr.proph, txn.tid)
	txn.mgr.deactivate(txn.sid, txn.tid)
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
