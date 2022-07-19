package txn

import (
	//"fmt"
	"sync"
	//"time"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/go-mvcc/config"
	"github.com/mit-pdos/go-mvcc/common"
	"github.com/mit-pdos/go-mvcc/gc"
	"github.com/mit-pdos/go-mvcc/index"
	"github.com/mit-pdos/go-mvcc/wrbuf"
	// "github.com/mit-pdos/go-mvcc/proph"
	/* Figure a way to support `cfmutex` */
	//"github.com/mit-pdos/go-mvcc/cfmutex"
	"github.com/tchajed/goose/machine"
)

type Txn struct {
	tid		uint64
	sid		uint64
	wrbuf	*wrbuf.WrBuf
	idx		*index.Index
	txnMgr	*TxnMgr
	p		machine.ProphId
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
	gc			*gc.GC
}

func MkTxnMgr() *TxnMgr {
	txnMgr := new(TxnMgr)
	txnMgr.latch = new(sync.Mutex)
	txnMgr.sites = make([]*TxnSite, config.N_TXN_SITES)
	for i := uint64(0); i < config.N_TXN_SITES; i++ {
		site := new(TxnSite)
		site.latch = new(sync.Mutex)
		site.tidsActive = make([]uint64, 0, 8)
		txnMgr.sites[i] = site
	}
	txnMgr.idx = index.MkIndex()
	txnMgr.gc = gc.MkGC(txnMgr.idx)
	return txnMgr
}

func (txnMgr *TxnMgr) New() *Txn {
	txnMgr.latch.Lock()

	/* Make a new txn. */
	txn := new(Txn)
	// TODO: MkWrBuf
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

func genTID(sid uint64) uint64 {
	var tid uint64

	/* Call `GetTSC` and round the result up to site ID boundary. */
	tid = grove_ffi.GetTSC()
	tid = ((tid + config.N_TXN_SITES) & ^(config.N_TXN_SITES - 1)) + sid
	// Below is the old (and wrong) version where we simply round the result,
	// up or down, to site ID boundary.
	// tid = (tid & ^(config.N_TXN_SITES - 1)) + sid

	/* Wait until TSC exceeds TID. */
	for grove_ffi.GetTSC() <= tid {
	}

	return tid
}

func (txnMgr *TxnMgr) activate(sid uint64) uint64 {
	site := txnMgr.sites[sid]
	site.latch.Lock()

	/**
	 * Justifying why TID is unique:
	 * For transactions with different SID (site ID), the last few bits are
	 * distinct.
	 * For transactions with the same SID, the loop below ensures the generated
	 * TIDs are strictly increasing.
	 */
	var tid uint64
	tid = genTID(sid)
	for tid <= site.tidLast {
		tid = genTID(sid)
	}
	/* Assume TID never overflow */
	machine.Assume(tid < 18446744073709551615)
	site.tidLast = tid

	/* Add `tid` to the set of active transactions */
	site.tidsActive = append(site.tidsActive, tid)

	site.latch.Unlock()
	return tid
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
	tidnew = genTID(sid)
	for tidnew <= site.tidLast {
		tidnew = genTID(sid)
	}
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

func (txnMgr *TxnMgr) runGC() {
	tidMin := txnMgr.getMinActiveTID()
	if tidMin < config.TID_SENTINEL {
		txnMgr.gc.Start(tidMin)
	}
}

func (txnMgr *TxnMgr) StartGC() {
	go func() {
		for {
			txnMgr.runGC()
			/* Goose: literal with kind INT */
			// time.Sleep(time.Duration(uint64(100)) * time.Millisecond)
		}
	}()
}

func (txn *Txn) Put(key, val uint64) bool {
	wrbuf := txn.wrbuf
	wrbuf.Put(key, val)

	/* TODO: Move this to `OwnAll`. */
	idx := txn.idx
	tuple := idx.GetTuple(key)

	/* Try to get the permission to update this tuple. */
	ret := tuple.Own(txn.tid)
	if ret != common.RET_SUCCESS {
		/* TODO: can retry a few times for RET_RETRY. */
		return false
	}

	return true
}

func (txn *Txn) Delete(key uint64) bool {
	wrbuf := txn.wrbuf
	wrbuf.Delete(key)

	/* TODO: Move this to `OwnAll`. */
	idx := txn.idx
	tuple := idx.GetTuple(key)

	/* Try to get the permission to update this tuple. */
	ret := tuple.Own(txn.tid)
	if ret != common.RET_SUCCESS {
		/* TODO: can retry a few times for RET_RETRY. */
		return false
	}

	return true
}

func (txn *Txn) Get(key uint64) (uint64, bool) {
	/* First try to find `key` in the local write set. */
	wrbuf := txn.wrbuf
	valb, del, found := wrbuf.Lookup(key)
	if found {
		return valb, !del
	}

	idx := txn.idx
	tuple := idx.GetTuple(key)
	val, ret := tuple.ReadVersion(txn.tid)

	return val, ret == common.RET_SUCCESS
}

func (txn *Txn) Begin() {
	tid := txn.txnMgr.activate(txn.sid)
	txn.tid = tid
	txn.wrbuf.Clear()
}

func (txn *Txn) Commit() {
	wrbuf := txn.wrbuf
	for i := uint64(0); i < wrbuf.Len(); i++ {
		key, val, del := wrbuf.GetEntAt(i)
		idx := txn.idx
		// If this additional `GetTuple` ever becomes a performance issue, use
		// another slice to store the `tuple` pointers.
		tuple := idx.GetTuple(key)
		/* TODO: Call KillVersion for tombstone. */
		if del {
			tuple.KillVersion(txn.tid)
		} else {
			tuple.AppendVersion(txn.tid, val)
		}
	}
	txn.txnMgr.deactivate(txn.sid, txn.tid)
}

func (txn *Txn) Abort() {
	wrbuf := txn.wrbuf
	for i := uint64(0); i < wrbuf.Len(); i++ {
		key, _, _ := wrbuf.GetEntAt(i)
		idx := txn.idx
		tuple := idx.GetTuple(key)
		tuple.Free(txn.tid)
	}
	txn.txnMgr.deactivate(txn.sid, txn.tid)
}

