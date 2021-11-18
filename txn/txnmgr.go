package txn

import (
	//"fmt"
	//"sync"
	//"time"
	"go-mvcc/config"
	"go-mvcc/tsc"
	"go-mvcc/gc"
	"go-mvcc/index"
	"go-mvcc/cfmutex"
)

type TxnSite struct {
	latch		*cfmutex.CFMutex
	tidLast		uint64
	tidsActive	[]uint64 /* or struct{} if Goose supports. */
	padding		[3]uint64
}

type TxnMgr struct {
	latch		*cfmutex.CFMutex
	sidCur		uint64
	sites		[]TxnSite
	idx			*index.Index
	gc			*gc.GC
}

func MkTxnMgr() *TxnMgr {
	txnMgr := new(TxnMgr)
	txnMgr.latch = new(cfmutex.CFMutex)
	txnMgr.sites = make([]TxnSite, config.N_TXN_SITES)
	for i := uint64(0); i < config.N_TXN_SITES; i++ {
		g := &txnMgr.sites[i]
		g.latch = new(cfmutex.CFMutex)
		g.tidsActive = make([]uint64, 0, 8)
	}
	txnMgr.idx = index.MkIndex()
	txnMgr.gc = gc.MkGC(txnMgr.idx)
	return txnMgr
}

func (txnMgr *TxnMgr) New() *Txn {
	txnMgr.latch.Lock()

	/* Make a new txn. */
	txn := new(Txn)
	txn.wset = make([]WrEnt, 0, 32)
	sid := txnMgr.sidCur
	txn.sid = sid
	txn.idx = txnMgr.idx
	txn.txnMgr = txnMgr

	txnMgr.sidCur = sid + 1
	if txnMgr.sidCur == config.N_TXN_SITES {
		txnMgr.sidCur = 0
	}

	txnMgr.latch.Unlock()
	return txn
}

func genTID(sid uint64) uint64 {
	tid := tsc.GetTSC()
	tid = (tid & ^(config.N_TXN_SITES - 1)) + sid
	return tid
}

func getSID(tid uint64) uint64 {
	sid := tid & (config.N_TXN_SITES - 1)
	return sid
}

func (txnMgr *TxnMgr) activate(sid uint64) uint64 {
	site := &txnMgr.sites[sid]
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
	site.tidLast = tid

	/* Add `tid` to the set of active transactions */
	site.tidsActive = append(site.tidsActive, tid)

	site.latch.Unlock()
	return tid
}

func findTID(tid uint64, tids []uint64) uint64 {
	var idx uint64 = 0
	for i, x := range tids {
		if tid == x {
			idx = uint64(i)
		}
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
func (txnMgr *TxnMgr) deactivate(tid uint64) {
	sid := getSID(tid)
	site := &txnMgr.sites[sid]
	site.latch.Lock()

	/* Remove `tid` from the set of active transactions. */
	idx := findTID(tid, site.tidsActive)
	swapWithEnd(site.tidsActive, idx)
	site.tidsActive = site.tidsActive[:len(site.tidsActive) - 1]

	site.latch.Unlock()
}

func (txnMgr *TxnMgr) getMinActiveTIDSite(sid uint64) uint64 {
	site := &txnMgr.sites[sid]
	site.latch.Lock()

	var min uint64 = config.TID_SENTINEL
	for _, tid := range site.tidsActive {
		if tid < min {
			min = tid
		}
	}

	site.latch.Unlock()
	return min
}

/**
 * This function returns the minimal TID of the active txns. If there is no
 * active txns, it returns `config.TID_SENTINEL`.
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
		site := &txnMgr.sites[sid]
		site.latch.Lock()
		n += uint64(len(site.tidsActive))
		site.latch.Unlock()
	}

	return n
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

func (txnMgr *TxnMgr) runGC() {
	tidMin := txnMgr.getMinActiveTID()
	if tidMin < config.TID_SENTINEL {
		txnMgr.gc.Start(tidMin)
	}
}

