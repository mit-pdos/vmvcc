//go:build ignore
// This test intends to test "how frequent timestamps collide if without our
// technique to make it unique". Comment out every line in tid.go except L13
// for comparison.
package main

import (
	"fmt"
	"github.com/goose-lang/goose/machine"
	"github.com/mit-pdos/vmvcc/txn"
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
		xStr, _ := txn.Read(uint64(i))
		x, err := strconv.ParseUint(xStr, 10, 64)
		if err != nil {
			panic(err)
		}
		if x > m {
			m = x
		}
	}

	randKey := machine.RandomUint64() % nkeys
	txn.Write(randKey, strconv.FormatUint(m+1, 10))
	return true
}

func incrThread(db *txn.TxnMgr) uint64 {
	txn := db.New()
	numIncrs := uint64(0)
	for i := 0; i < 1_000_000; i += 1 {
		committed := txn.Run(doIncr)
		if committed {
			numIncrs += 1
		}
	}
	return numIncrs
}

func putZero(db *txn.TxnMgr) int {
	t := db.New()
	v := new(int)

	committed := t.Run(func(t *txn.Txn) bool {
		for i := 0; i < nkeys; i += 1 {
			t.Write(uint64(i), "0")
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

	committed := t.Run(func(t *txn.Txn) bool {
		var m uint64
		for i := 0; i < nkeys; i += 1 {
			xStr, _ := t.Read(uint64(i))
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
