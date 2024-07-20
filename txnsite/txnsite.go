package txnsite

import (
	"github.com/goose-lang/primitive"
	"github.com/mit-pdos/vmvcc/cfmutex"
	"github.com/mit-pdos/vmvcc/tid"
)

type TxnSite struct {
	latch *cfmutex.CFMutex
	sid   uint64
	// ID of this transaction site
	tids []uint64
	// Active transaction IDs.
	padding [4]uint64
	// TODO: should only need 3, change it once performance is tested.
}

func MkTxnSite(sid uint64) *TxnSite {
	site := new(TxnSite)
	site.latch = new(cfmutex.CFMutex)
	site.tids = make([]uint64, 0, 8)
	site.sid = sid

	return site
}

// @activate adds @tid to the set of active transaction IDs.
func (site *TxnSite) Activate() uint64 {
	site.latch.Lock()

	var t uint64
	t = tid.GenTID(site.sid)
	// Assume TID never overflow.
	primitive.Assume(t < 18446744073709551615)

	site.tids = append(site.tids, t)

	site.latch.Unlock()
	return t
}

func findTID(tid uint64, tids []uint64) uint64 {
	// Require @tids contains @tid.
	var idx uint64 = 0
	for tid != tids[idx] {
		idx++
	}

	return idx
}

func swapWithEnd(xs []uint64, i uint64) {
	// Require (1) @xs not empty, (2) @i < len(@xs).
	tmp := xs[len(xs)-1]
	xs[len(xs)-1] = xs[i]
	xs[i] = tmp
}

// @deactivate removes @tid from the set of active transaction IDs.
func (site *TxnSite) Deactivate(tid uint64) {
	// Require @site.tids contains @tid.
	site.latch.Lock()

	// Remove @tid from the set of active transactions.
	idx := findTID(tid, site.tids)
	swapWithEnd(site.tids, idx)
	site.tids = site.tids[:len(site.tids)-1]

	site.latch.Unlock()
}

// @GetSafeTS returns a per-site lower bound on the active/future transaction
// IDs.
func (site *TxnSite) GetSafeTS() uint64 {
	site.latch.Lock()

	var tidnew uint64
	tidnew = tid.GenTID(site.sid)
	primitive.Assume(tidnew < 18446744073709551615)

	var tidmin uint64 = tidnew
	for _, tid := range site.tids {
		if tid < tidmin {
			tidmin = tid
		}
	}

	site.latch.Unlock()
	return tidmin
}
