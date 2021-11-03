package txn

import (
	"sync"
	"go-mvcc/index"
)

type TxnMgr struct {
	latch		*sync.Mutex
	tidCur		uint64
	tidsActive	map[uint64]bool /* or struct{} if Goose has support. */
	idx			*index.Index
}

func MkTxnMgr() *TxnMgr {
	txnMgr := new(TxnMgr)
	txnMgr.latch = new(sync.Mutex)
	txnMgr.tidsActive = make(map[uint64]bool)
	txnMgr.idx = index.MkIndex()
	return txnMgr
}

func (txnMgr *TxnMgr) New() Txn {
	txnMgr.latch.Lock()
	txnMgr.tidCur++

	/* Make a new txn. */
	tidNew := txnMgr.tidCur
	wsetNew := make(map[uint64]WrEnt)
	txn := Txn{tidNew, wsetNew, txnMgr.idx}

	/* Add `tidNew` to the set of active txns. */
	txnMgr.tidsActive[tidNew] = true

	txnMgr.latch.Unlock()
	return txn
}

