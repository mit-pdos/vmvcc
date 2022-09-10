package main

import (
	"time"
	"fmt"
	"runtime/pprof"
	"flag"
	"os"
	"log"
	"math/rand"
	"github.com/mit-pdos/go-mvcc/txn"
)

var done bool

func populateDataBody(txn *txn.Txn, key uint64) bool {
	txn.Put(key, 2 * key + 1)
	return true
}

func populateData(txnMgr *txn.TxnMgr, rkeys uint64) {
	t := txnMgr.New()
	for k := uint64(0); k < rkeys; k++ {
		body := func(txn *txn.Txn) bool {
			return populateDataBody(txn, k)
		}
		t.DoTxn(body)
	}
}

func readerBody(txn *txn.Txn, keys []uint64) bool {
	for _, k := range(keys) {
		txn.Put(k, k + 1)
	}
	return true
}

func reader(txnMgr *txn.TxnMgr, src rand.Source, chCommitted, chTotal chan uint64, nkeys int, rkeys uint64) {
	var committed uint64 = 0
	var total uint64 = 0
	r := int64(rkeys)
	rd := rand.New(src)
	keys := make([]uint64, nkeys)
	t := txnMgr.New()
	for !done {
		for i := 0; i < nkeys; i++ {
			k := uint64(rd.Int63n(r))
			keys[i] = k
		}
		body := func(txn *txn.Txn) bool {
			return readerBody(txn, keys)
		}
		res := t.DoTxn(body)
		if res {
			committed++
		}
		total++
	}
	chCommitted <-committed
	chTotal <-total
}

func main() {
	txnMgr := txn.MkTxnMgr()
	//txnMgr.StartGC()

	var nthrds int
	var nkeys int
	var rkeys uint64
	var cpuprof string
	var exp bool
	flag.IntVar(&nthrds, "nthrds", 1, "number of threads")
	flag.IntVar(&nkeys, "nkeys", 1, "number of keys accessed per txn")
	flag.Uint64Var(&rkeys, "rkeys", 1000, "access keys within [0:rkeys)")
	flag.StringVar(&cpuprof, "cpuprof", "cpu.prof", "write cpu profile to cpuprof")
	flag.BoolVar(&exp, "exp", false, "print only experimental data")
	flag.Parse()

	chCommitted := make(chan uint64)
	chTotal := make(chan uint64)

	if cpuprof != "" {
		f, err := os.Create(cpuprof)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	populateData(txnMgr, rkeys)
	if !exp {
		fmt.Printf("Database populated.\n")
	}

	done = false
	for i := 0; i < nthrds; i++ {
		src := rand.NewSource(int64(i))
		go reader(txnMgr, src, chCommitted, chTotal, nkeys, rkeys)
	}
	time.Sleep(3 * time.Second)
	done = true

	var c uint64 = 0
	var t uint64 = 0
	for i := 0; i < nthrds; i++ {
		c += <-chCommitted
		t += <-chTotal
	}
	rate := float64(c) / float64(t)

	if exp {
		fmt.Printf("%d, %d, %d, %d, %d, %f\n", nthrds, nkeys, rkeys, c, t, rate)
	} else {
		fmt.Printf("committed / total = %d / %d (%f).\n", c, t, rate)
	}
}

