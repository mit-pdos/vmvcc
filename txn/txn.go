package txn

import (
	//"fmt"
	"sync"
	//"time"
	"github.com/mit-pdos/go-mvcc/config"
	"github.com/mit-pdos/go-mvcc/common"
	"github.com/mit-pdos/go-mvcc/tsc"
	"github.com/mit-pdos/go-mvcc/gc"
	"github.com/mit-pdos/go-mvcc/tuple"
	"github.com/mit-pdos/go-mvcc/index"
	/* Figure a way to support `cfmutex` */
	//"github.com/mit-pdos/go-mvcc/cfmutex"
)

type DBVal struct {
	tomb bool
	val  uint64
}

/**
 * We need `key` as to match in the local write set
 */
type WrEnt struct {
	key		uint64
	val		DBVal
	tuple	*tuple.Tuple
}

/**
 * `wset` as a `map[uint64]WrEnt` has the issue of allocating and deallocating
 * many `WrEnt`.
 */
type Txn struct {
	tid		uint64
	wset	[]WrEnt
	sid		uint64
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
	txn.wset = make([]WrEnt, 0, 32)
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
	tid = tsc.GetTSC()
	tid = (tid & ^(config.N_TXN_SITES - 1)) + sid
	return tid
}

func getSID(tid uint64) uint64 {
	sid := tid & (config.N_TXN_SITES - 1)
	return sid
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

func matchLocalWrites(key uint64, wset []WrEnt) (uint64, bool) {
	var pos uint64 = 0
	for {
		if pos >= uint64(len(wset)) {
			break
		}
		if key == wset[pos].key {
			break
		}
		pos++
	}

	found := pos < uint64(len(wset))
	return pos, found
}

func (txn *Txn) Put(key, val uint64) bool {
	/* First try to find `key` in the local write set. */
	pos, found := matchLocalWrites(key, txn.wset)
	if found {
		went := &txn.wset[pos]
		went.val = DBVal{
			tomb : false,
			val  : val,
		}
		return true
	}

	idx := txn.idx
	tuple := idx.GetTuple(key)

	/* Try to get the permission to update this tuple. */
	ret := tuple.Own(txn.tid)
	if ret != common.RET_SUCCESS {
		/* TODO: can retry a few times for RET_RETRY. */
		return false
	}

	/* Add the key-value pair to the local write set. */
	dbval := DBVal{
		tomb : false,
		val  : val,
	}
	txn.wset = append(txn.wset, WrEnt{key: key, val: dbval, tuple: tuple})

	return true
}

func (txn *Txn) Delete(key uint64) bool {
	/* First try to find `key` in the local write set. */
	pos, found := matchLocalWrites(key, txn.wset)
	if found {
		went := &txn.wset[pos]
		went.val = DBVal{
			tomb : true,
		}
		return true
	}

	idx := txn.idx
	tuple := idx.GetTuple(key)

	/* Try to get the permission to update this tuple. */
	ret := tuple.Own(txn.tid)
	if ret != common.RET_SUCCESS {
		/* TODO: can retry a few times for RET_RETRY. */
		return false
	}

	/* Add the key-value pair to the local write set. */
	dbval := DBVal{
		tomb : true,
	}
	txn.wset = append(txn.wset, WrEnt{key: key, val: dbval, tuple: tuple})

	return true
}

func (txn *Txn) Get(key uint64) (uint64, bool) {
	/* First try to find `key` in the local write set. */
	pos, found := matchLocalWrites(key, txn.wset)
	if found {
		dbval := txn.wset[pos].val
		return dbval.val, !dbval.tomb
	}

	idx := txn.idx
	tuple := idx.GetTuple(key)
	val, ret := tuple.ReadVersion(txn.tid)

	return val, ret == common.RET_SUCCESS
}

func (txn *Txn) Begin() {
	tid := txn.txnMgr.activate(txn.sid)
	txn.tid = tid
	txn.wset = txn.wset[:0]
}

func (txn *Txn) Commit() {
	for _, wrent := range txn.wset {
		dbval := wrent.val
		tuple := wrent.tuple
		/* TODO: Call KillVersion for tombstone. */
		tuple.AppendVersion(txn.tid, dbval.val)
	}
	txn.txnMgr.deactivate(txn.tid)
}

func (txn *Txn) Abort() {
	for _, wrent := range txn.wset {
		tuple := wrent.tuple
		tuple.Free(txn.tid)
	}
	txn.txnMgr.deactivate(txn.tid)
}

