package vmvcc

import (
	"github.com/mit-pdos/vmvcc/index"
	"github.com/mit-pdos/vmvcc/trusted_proph"
	"github.com/mit-pdos/vmvcc/txnsite"
	"github.com/mit-pdos/vmvcc/wrbuf"
	"github.com/tchajed/goose/machine"
)

type Txn struct {
	// Transaction ID.
	tid uint64
	// Transaction site this transaction uses.
	site *txnsite.TxnSite
	// Write buffer.
	wrbuf *wrbuf.WrBuf
	// Pointer to the index.
	idx *index.Index
	// Global prophecy variable (for verification purpose).
	proph machine.ProphId
}

func (txn *Txn) Write(key uint64, val string) {
	wrbuf := txn.wrbuf
	wrbuf.Put(key, val)
}

func (txn *Txn) Delete(key uint64) bool {
	wrbuf := txn.wrbuf
	wrbuf.Delete(key)

	// To support SQL in the future, @Delete should return false when not found.
	return true
}

func (txn *Txn) Read(key uint64) (string, bool) {
	// First try to find @key in the local write set.
	wrbuf := txn.wrbuf
	valb, wr, found := wrbuf.Lookup(key)
	if found {
		return valb, wr
	}

	idx := txn.idx
	tuple := idx.GetTuple(key)
	tuple.ReadWait(txn.tid)
	trusted_proph.ResolveRead(txn.proph, txn.tid, key)
	val, found := tuple.ReadVersion(txn.tid)

	return val, found
}

func (txn *Txn) begin() {
	tid := txn.site.Activate()
	txn.tid = tid
	txn.wrbuf.Clear()
}

func (txn *Txn) acquire() bool {
	ok := txn.wrbuf.OpenTuples(txn.tid, txn.idx)
	return ok
}

func (txn *Txn) commit() {
	trusted_proph.ResolveCommit(txn.proph, txn.tid, txn.wrbuf)
	txn.wrbuf.UpdateTuples(txn.tid)
	txn.site.Deactivate(txn.tid)
}

func (txn *Txn) abort() {
	trusted_proph.ResolveAbort(txn.proph, txn.tid)
	txn.site.Deactivate(txn.tid)
}

func (txn *Txn) Run(body func(txn *Txn) bool) bool {
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
