package main

import (
	"fmt"
	"github.com/mit-pdos/go-mvcc/txn"
	"github.com/tchajed/goose/machine"
	"strconv"
	"sync"
	"time"
)

// XXX: need to use many keys to get observably bad behavior from duplicate
// TIDs, because with one key, if there are two transactions using the same tid,
// one of them is likely to end up aborting because wrbuf.OpenTuples doesn't
// retry in case tuple.Own() returns RET_RETRY.

const nkeys = 16

func doIncr(txn *txn.Txn) bool {
	var m uint64

	for i := 0; i < nkeys; i += 1 {
		xStr, _ := txn.Get(uint64(i))
		x, err := strconv.ParseUint(xStr, 10, 64)
		if err != nil {
			panic(err)
		}
		if x > m {
			m = x
		}
	}

	randKey := machine.RandomUint64() % nkeys
	txn.Put(randKey, strconv.FormatUint(m+1, 10))
	return true
}

func incrThread(db *txn.TxnMgr) uint64 {
	txn := db.New()
	numIncrs := uint64(0)
	for i := 0; i < 1_000_000; i += 1 {
		committed := txn.DoTxn(doIncr)
		if committed {
			numIncrs += 1
		}
	}
	return numIncrs
}

func putZero(db *txn.TxnMgr) int {
	t := db.New()
	v := new(int)

	committed := t.DoTxn(func(t *txn.Txn) bool {
		for i := 0; i < nkeys; i += 1 {
			t.Put(uint64(i), "0")
		}
		return true
	})
	if !committed {
		panic("put aborted")
	}

	return *v
}

func getValue(db *txn.TxnMgr) uint64 {
	t := db.New()
	v := new(uint64)

	committed := t.DoTxn(func(t *txn.Txn) bool {
		var m uint64
		for i := 0; i < nkeys; i += 1 {
			xStr, _ := t.Get(uint64(i))
			x, err := strconv.ParseUint(xStr, 10, 64)
			if err != nil {
				panic(err)
			}
			if x > m {
				m = x
			}
		}
		*v = m
		return true
	})

	if !committed {
		panic("read aborted")
	}

	return *v
}

func main() {
	n := 8
	numIncrss := make([]uint64, n)
	db := txn.MkTxnMgr()
	db.ActivateGC()

	putZero(db)
	time.Sleep(10 * time.Millisecond)

	wg := new(sync.WaitGroup)
	wg.Add(n)
	for i, _ := range numIncrss {
		i := i
		go func() {
			numIncrss[i] = incrThread(db)
			wg.Done()
		}()
	}

	wg.Wait()
	var s uint64
	for _, numIncrs := range numIncrss {
		s += numIncrs
	}
	fmt.Printf("numIncrs = %d, val = %d\n", s, getValue(db))
}
