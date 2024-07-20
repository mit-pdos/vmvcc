package vmvcc

import (
	"github.com/goose-lang/primitive"
	"github.com/mit-pdos/vmvcc/cfmutex"
	"github.com/mit-pdos/vmvcc/config"
	"github.com/mit-pdos/vmvcc/index"
	"github.com/mit-pdos/vmvcc/tid"
	"github.com/mit-pdos/vmvcc/txnsite"
	"github.com/mit-pdos/vmvcc/wrbuf"
)

type DB struct {
	// Mutex protecting @sid.
	latch *cfmutex.CFMutex
	// Next site ID to assign.
	sid uint64
	// All transaction sites.
	sites []*txnsite.TxnSite
	// Index.
	idx *index.Index
	// Global prophecy variable (for verification purpose).
	proph primitive.ProphId
}

func MkDB() *DB {
	proph := primitive.NewProph()
	db := &DB{proph: proph}
	db.latch = new(cfmutex.CFMutex)
	db.sites = make([]*txnsite.TxnSite, config.N_TXN_SITES)

	// Call this once to establish invariants.
	tid.GenTID(0)
	for i := uint64(0); i < config.N_TXN_SITES; i++ {
		site := txnsite.MkTxnSite(i)
		db.sites[i] = site
	}
	db.idx = index.MkIndex()

	return db
}

func (db *DB) NewTxn() *Txn {
	db.latch.Lock()

	txn := &Txn{proph: db.proph}
	txn.site = db.sites[db.sid]
	txn.wrbuf = wrbuf.MkWrBuf()
	txn.idx = db.idx

	db.sid = db.sid + 1
	if db.sid >= config.N_TXN_SITES {
		db.sid = 0
	}

	db.latch.Unlock()
	return txn
}

// @GetSafeTS returns a lower bound on the active/future transaction IDs.
func (db *DB) getSafeTS() uint64 {
	var min uint64 = config.TID_SENTINEL
	// TODO: A more elegant way is to use range
	for sid := uint64(0); sid < config.N_TXN_SITES; sid++ {
		site := db.sites[sid]
		tid := site.GetSafeTS()
		if tid < min {
			min = tid
		}
	}

	return min
}

func (db *DB) gc() {
	tidMin := db.getSafeTS()
	if tidMin < config.TID_SENTINEL {
		db.idx.DoGC(tidMin)
	}
}

func (db *DB) ActivateGC() {
	go func() {
		for {
			db.gc()
			primitive.Sleep(1 * 1000000)
		}
	}()
}

func (db *DB) Run(body func(txn *Txn) bool) bool {
	txn := db.NewTxn()
	return txn.Run(body)
}
