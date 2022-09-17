package tpcc

/**
 * Axiomatized transaction interface.
 * This file will be removed once we adapt our implementation and proof to this
 * interface.
 */
type Txn struct {
}

type TxnMgr struct {
}

func MkTxnMgr() *TxnMgr {
	txnMgr := new(TxnMgr)
	return txnMgr
}

func (txnMgr *TxnMgr) New() *Txn {
	txn := new(Txn)
	return txn
}

func (txn *Txn) Read(mapid uint64, key []byte) ([]byte, bool) {
	return nil, true
}

func (txn *Txn) Write(mapid uint64, key, val []byte) {
}

func (txn *Txn) Delete(mapid uint64, key []byte) bool {
	return true
}

func (txn *Txn) DoTxn(body func(txn *Txn) bool) bool {
	return true
}
