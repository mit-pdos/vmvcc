package main

import (
	"time"
	"fmt"
	"sync"
	"runtime/pprof"
	"flag"
	"os"
	"log"
	"math/rand"
	"go-mvcc/txn"
)

var done bool

func clientMap(mu *sync.Mutex, chTotal chan uint64) {
	var t uint64 = 0
	var r int64 = 1000
	m := make(map[uint64]uint64)
	for !done {
		mu.Lock()
		k := uint64(rand.Int63n(r))
		m[k] = k * 2 + 1
		mu.Unlock()
		t++
	}
	chTotal <-t
}

func client(txnMgr *txn.TxnMgr, src rand.Source, chCommitted, chTotal chan uint64) {
	var c uint64 = 0
	var t uint64 = 0
	var r int64 = 100000
	txn := txnMgr.New()
	rd := rand.New(src)
	for !done {
		txn.Begin()
		canCommit := true
		for i := 0; i < 5; i++ {
			k := uint64(rd.Int63n(r))
			ok := txn.Put(k, k * 2 + 1)
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
	var nthrd int = 2

	chCommitted := make(chan uint64)
	chTotal := make(chan uint64)

	/* Start the CPU profiler. */
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
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
	for i := 0; i < nthrd; i++ {
		src := rand.NewSource(int64(i))
		go client(txnMgr, src, chCommitted, chTotal)
	}
	time.Sleep(10 * time.Second)
	done = true

	var c uint64 = 0
	var t uint64 = 0
	for i := 0; i < nthrd; i++ {
		c += <-chCommitted
		t += <-chTotal
	}
	rate := float64(c) / float64(t)
	fmt.Printf("committed / total = %d / %d (%f).\n", c, t, rate)

	/*
	mu := new(sync.Mutex)
	done = false
	for i := 0; i < nthrd; i++ {
		go clientMap(mu, chTotal)
	}
	time.Sleep(5 * time.Second)
	done = true

	t = 0
	for i := 0; i < nthrd; i++ {
		t += <-chTotal
	}
	fmt.Printf("total = %d.\n", t)
	*/
}

