package txn

import (
	//"fmt"
	"sync"
	//"time"
	"github.com/mit-pdos/go-mvcc/config"
	"github.com/mit-pdos/go-mvcc/index"
	"github.com/mit-pdos/go-mvcc/wrbuf"
	"github.com/mit-pdos/go-mvcc/trusted_proph"
	"github.com/mit-pdos/go-mvcc/tid"
	/* Figure a way to support `cfmutex` */
	//"github.com/mit-pdos/go-mvcc/cfmutex"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/tchajed/goose/machine"
)

type Txn struct {
	tid		uint64
	sid		uint64
	wrbuf	*wrbuf.WrBuf
	idx		*index.Index
	txnMgr	*TxnMgr
}

type TxnSite struct {
	latch		*sync.Mutex
	tidLast		uint64
	tidsActive	[]uint64 /* or struct{} if Goose supports. */
	padding		[3]uint64
}

type TxnMgr struct {
	latch		*sync.Mutex
	sidCur		uint64
	sites		[]*TxnSite
	idx			*index.Index
	p			machine.ProphId
}

func MkTxnMgr() *TxnMgr {
	txnMgr := new(TxnMgr)
	txnMgr.latch = new(sync.Mutex)
	txnMgr.sites = make([]*TxnSite, config.N_TXN_SITES)
	/* Call this once for establishing invariants. */
	tid.GenTID(0)
	for i := uint64(0); i < config.N_TXN_SITES; i++ {
		site := new(TxnSite)
		site.latch = new(sync.Mutex)
		site.tidsActive = make([]uint64, 0, 8)
		txnMgr.sites[i] = site
	}
	txnMgr.p = machine.NewProph()
	txnMgr.idx = index.MkIndex()
	return txnMgr
}

func (txnMgr *TxnMgr) New() *Txn {
	txnMgr.latch.Lock()

	/* Make a new txn. */
	txn := new(Txn)
	txn.wrbuf = wrbuf.MkWrBuf()
	sid := txnMgr.sidCur
	txn.sid = sid
	txn.idx = txnMgr.idx
	txn.txnMgr = txnMgr

	txnMgr.sidCur = sid + 1
	if txnMgr.sidCur >= config.N_TXN_SITES {
		txnMgr.sidCur = 0
	}

	txnMgr.latch.Unlock()
	return txn
}

func (txnMgr *TxnMgr) activate(sid uint64) uint64 {
	site := txnMgr.sites[sid]
	site.latch.Lock()

	var t uint64
	t = tid.GenTID(sid)
	/* Assume TID never overflow */
	machine.Assume(t < 18446744073709551615)
	/* TODO: remove this when removing `tidLast` from the proof. */
	site.tidLast = t

	/* Add `tid` to the set of active transactions */
	site.tidsActive = append(site.tidsActive, t)

	site.latch.Unlock()
	return t
}

func findTID(tid uint64, tids []uint64) uint64 {
	var idx uint64 = 0
	for {
		tidx := tids[idx]
		if tid == tidx {
			break
		}
		idx++
	}

	return idx
}

/**
 * Precondition:
 * 1. `xs` not empty.
 * 2. `i < len(xs)`
 */
func swapWithEnd(xs []uint64, i uint64) {
	tmp := xs[len(xs) - 1]
	xs[len(xs) - 1] = xs[i]
	xs[i] = tmp
}

/**
 * This function is called by `Txn` at commit/abort time.
 * Precondition:
 * 1. The set of active transactions contains `tid`.
 */
func (txnMgr *TxnMgr) deactivate(sid uint64, tid uint64) {
	site := txnMgr.sites[sid]
	site.latch.Lock()

	/* Remove `tid` from the set of active transactions. */
	idx := findTID(tid, site.tidsActive)
	swapWithEnd(site.tidsActive, idx)
	site.tidsActive = site.tidsActive[:len(site.tidsActive) - 1]

	site.latch.Unlock()
}

func (txnMgr *TxnMgr) getMinActiveTIDSite(sid uint64) uint64 {
	site := txnMgr.sites[sid]
	site.latch.Lock()

	var tidnew uint64
	tidnew = tid.GenTID(sid)
	machine.Assume(tidnew < 18446744073709551615)

	site.tidLast = tidnew

	var tidmin uint64 = tidnew
	for _, tid := range site.tidsActive {
		if tid < tidmin {
			tidmin = tid
		}
	}

	site.latch.Unlock()
	return tidmin
}

/**
 * This function returns a lower bound of the active TID.
 */
func (txnMgr *TxnMgr) getMinActiveTID() uint64 {
	var min uint64 = config.TID_SENTINEL
	for sid := uint64(0); sid < config.N_TXN_SITES; sid++ {
		tid := txnMgr.getMinActiveTIDSite(sid)
		if tid < min {
			min = tid
		}
	}

	return min
}

/**
 * Probably only used for testing and profiling.
 */
func (txnMgr *TxnMgr) getNumActiveTxns() uint64 {
	var n uint64 = 0
	for sid := uint64(0); sid < config.N_TXN_SITES; sid++ {
		site := txnMgr.sites[sid]
		site.latch.Lock()
		n += uint64(len(site.tidsActive))
		site.latch.Unlock()
	}

	return n
}

func (txnMgr *TxnMgr) gc() {
	tidMin := txnMgr.getMinActiveTID()
	if tidMin < config.TID_SENTINEL {
		txnMgr.idx.DoGC(tidMin)
	}
}

func (txnMgr *TxnMgr) ActivateGC() {
	go func() {
		for {
			txnMgr.gc()
			grove_ffi.Sleep(uint64(100) * uint64(1000000))
		}
	}()
}

func (txn *Txn) Put(key, val uint64) {
	wrbuf := txn.wrbuf
	wrbuf.Put(key, val)
}

func (txn *Txn) Delete(key uint64) bool {
	wrbuf := txn.wrbuf
	wrbuf.Delete(key)

	/* TODO: `Delete` should return false when no such key. */
	return true
}

func (txn *Txn) Get(key uint64) (uint64, bool) {
	/* First try to find `key` in the local write set. */
	wrbuf := txn.wrbuf
	valb, wr, found := wrbuf.Lookup(key)
	if found {
		return valb, wr
	}

	idx := txn.idx
	tuple := idx.GetTuple(key)
	tuple.ReadWait(txn.tid)
	trusted_proph.ResolveRead(txn.txnMgr.p, txn.tid, key)
	val, found := tuple.ReadVersion(txn.tid)

	return val, found
}

func (txn *Txn) Begin() {
	tid := txn.txnMgr.activate(txn.sid)
	txn.tid = tid
	txn.wrbuf.Clear()
}

func (txn *Txn) acquire() bool {
	ok := txn.wrbuf.OpenTuples(txn.tid, txn.idx)
	return ok
}

func (txn *Txn) Commit() {
	trusted_proph.ResolveCommit(txn.txnMgr.p, txn.tid, txn.wrbuf)
	txn.wrbuf.UpdateTuples(txn.tid)
	txn.txnMgr.deactivate(txn.sid, txn.tid)
}

func (txn *Txn) Abort() {
	trusted_proph.ResolveAbort(txn.txnMgr.p, txn.tid)
	txn.txnMgr.deactivate(txn.sid, txn.tid)
}

func (txn *Txn) DoTxn(body func(txn *Txn) bool) bool {
	txn.Begin()
	cmt := body(txn)
	if !cmt {
		txn.Abort()
		return false
	}
	ok := txn.acquire()
	if !ok {
		txn.Abort()
		return false
	}
	txn.Commit()
	return true
}

/* TODO: Move these to examples. */
func SwapSeq(txn *Txn) bool {
	v1, _ := txn.Get(10)
	v2, _ := txn.Get(20)
	txn.Put(10, v2)
	txn.Put(20, v1)
	return true
}

func Swap(txn *Txn) bool {
	return txn.DoTxn(SwapSeq)
}

