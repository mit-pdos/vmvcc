package txn

import (
	"sync/atomic"
	"go-mvcc/index"
)

type TxnMgr struct {
	tidCur	uint64
}

func MkTxnMgr() *TxnMgr {
	txnMgr := &TxnMgr{0}
	return txnMgr
}

func (txnMgr *TxnMgr) New(idx *index.Index) Txn {
	tidNew := atomic.AddUint64(&txnMgr.tidCur, 1)
	wsetNew := make([]WrEnt, 16)
	txn := Txn{tidNew, wsetNew, idx}
	return txn
}

