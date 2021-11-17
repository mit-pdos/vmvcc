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

type TIDGen struct {
	latch		*cfmutex.CFMutex
	tidLast		uint64
	tidsActive	[]uint64 /* or struct{} if Goose supports. */
	padding		[3]uint64
}

type TxnMgr struct {
	latch		*cfmutex.CFMutex
	tokenCur	uint64
	tidGens		[]TIDGen
	idx			*index.Index
	gc			*gc.GC
}

func MkTxnMgr() *TxnMgr {
	txnMgr := new(TxnMgr)
	txnMgr.latch = new(cfmutex.CFMutex)
	txnMgr.tidGens = make([]TIDGen, config.MAX_TOKEN)
	for i := uint64(0); i < config.MAX_TOKEN; i++ {
		g := &txnMgr.tidGens[i]
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
	token := txnMgr.tokenCur
	txn.token = token
	txn.idx = txnMgr.idx
	txn.txnMgr = txnMgr

	txnMgr.tokenCur = token + 1
	if txnMgr.tokenCur == config.MAX_TOKEN {
		txnMgr.tokenCur = 0
	}

	txnMgr.latch.Unlock()
	return txn
}

func genTID(token uint64) uint64 {
	tid := tsc.GetTSC()
	tid = (tid & ^(config.MAX_TOKEN - 1)) + token
	return tid
}

func getToken(tid uint64) uint64 {
	token := tid & (config.MAX_TOKEN - 1)
	return token
}

func (txnMgr *TxnMgr) activate(token uint64) uint64 {
	tidGen := &txnMgr.tidGens[token]
	tidGen.latch.Lock()

	/**
	 * Justifying why TID is unique:
	 * For transactions with different token, the last few bits are distinct.
	 * For transactions with the same token, the loop below ensures the
	 * generated TIDs are strictly increasing.
	 */
	var tid uint64
	tid = genTID(token)
	for tid <= tidGen.tidLast {
		tid = genTID(token)
	}
	tidGen.tidLast = tid

	/* Add `tid` to the set of active transactions */
	tidGen.tidsActive = append(tidGen.tidsActive, tid)

	tidGen.latch.Unlock()
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
	token := getToken(tid)
	tidGen := &txnMgr.tidGens[token]
	tidGen.latch.Lock()

	/* Remove `tid` from the set of active transactions. */
	idx := findTID(tid, tidGen.tidsActive)
	swapWithEnd(tidGen.tidsActive, idx)
	tidGen.tidsActive = tidGen.tidsActive[:len(tidGen.tidsActive) - 1]

	tidGen.latch.Unlock()
}

func (txnMgr *TxnMgr) getMinActiveTIDShard(sid uint64) uint64 {
	tidShard := &txnMgr.tidGens[sid]
	tidShard.latch.Lock()

	var min uint64 = config.TID_SENTINEL
	for _, tid := range tidShard.tidsActive {
		if tid < min {
			min = tid
		}
	}

	tidShard.latch.Unlock()
	return min
}

/**
 * This function returns the minimal TID of the active txns. If there is no
 * active txns, it returns `config.TID_SENTINEL`.
 */
func (txnMgr *TxnMgr) getMinActiveTID() uint64 {
	var min uint64 = config.TID_SENTINEL
	for sid := uint64(0); sid < config.MAX_TOKEN; sid++ {
		tid := txnMgr.getMinActiveTIDShard(sid)
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
	for sid := uint64(0); sid < config.MAX_TOKEN; sid++ {
		tidShard := &txnMgr.tidGens[sid]
		tidShard.latch.Lock()
		n += uint64(len(tidShard.tidsActive))
		tidShard.latch.Unlock()
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

