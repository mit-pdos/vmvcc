package main

import (
	"time"
	"fmt"
	"runtime/pprof"
	"flag"
	"os"
	"log"
	"math/rand"
	"go-mvcc/txn"
)

var done bool

func writer(txnMgr *txn.TxnMgr, src rand.Source, chCommitted, chTotal chan uint64, nkeys int, rkeys uint64) {
	var c uint64 = 0
	var t uint64 = 0
	r := int64(rkeys)
	txn := txnMgr.New()
	rd := rand.New(src)
	for !done {
		txn.Begin()
		canCommit := true
		for i := 0; i < nkeys; i++ {
			k := uint64(rd.Int63n(r))
			ok := txn.Put(k, 2 * k + 1)
			if !ok {
				canCommit = false
				break
			}
		}
		if canCommit {
			c++
			txn.Commit()
		} else {
			txn.Abort()
		}
		t++
	}
	chCommitted <-c
	chTotal <-t
}

func main() {
	txnMgr := txn.MkTxnMgr()
	txnMgr.StartGC()

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

	done = false
	for i := 0; i < nthrds; i++ {
		src := rand.NewSource(int64(i))
		go writer(txnMgr, src, chCommitted, chTotal, nkeys, rkeys)
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

