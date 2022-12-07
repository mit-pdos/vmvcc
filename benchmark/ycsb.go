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
	// "github.com/mit-pdos/go-mvcc/tplock"
)

var done bool
var szrec int = 100

func populateDataBody(txn *txn.Txn, key uint64) bool {
	s := string(make([]byte, szrec))
	txn.Put(key, s)
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

func longReaderBody(txn *txn.Txn, rkeys uint64) bool {
	for k := uint64(0); k < rkeys; k++ {
		txn.Get(k)
	}
	return true
}

func longReader(txnMgr *txn.TxnMgr, rkeys uint64) {
	t := txnMgr.New()
	for !done {
		body := func(txn *txn.Txn) bool {
			return longReaderBody(txn, rkeys)
		}
		t.DoTxn(body)
	}
}

func workerBody(txn *txn.Txn, keys []uint64, rw []bool) bool {
	for i, k := range(keys) {
		if rw[i] {
			txn.Get(k)
		} else {
			s := string(make([]byte, szrec))
			txn.Put(k, s)
		}
	}
	return true
}

func worker(txnMgr *txn.TxnMgr, src rand.Source, chCommitted, chTotal chan uint64, nkeys int, rkeys uint64, rdratio uint64) {
	var committed uint64 = 0
	var total uint64 = 0
	r := int64(rkeys)
	rd := rand.New(src)
	keys := make([]uint64, nkeys)
	rw := make([]bool, nkeys)
	t := txnMgr.New()
	for !done {
		for i := 0; i < nkeys; i++ {
			k := uint64(rd.Int63n(r))
			x := uint64(rd.Int63n(int64(100)))
			keys[i] = k
			if x < rdratio {
				rw[i] = true
			} else {
				rw[i] = false
			}
		}
		body := func(txn *txn.Txn) bool {
			return workerBody(txn, keys, rw)
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

func readerBody(txn *txn.Txn, keys []uint64) bool {
	for _, k := range(keys) {
		_, ok := txn.Get(k)
		if !ok {
			return false
		}
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

func writerBody(txn *txn.Txn, keys []uint64) bool {
	for _, k := range(keys) {
		s := string(make([]byte, szrec))
		txn.Put(k, s)
	}
	return true
}

func writer(txnMgr *txn.TxnMgr, src rand.Source, chCommitted, chTotal chan uint64, nkeys int, rkeys uint64) {
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
			return writerBody(txn, keys)
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
	txnMgr.ActivateGC()

	var nthrds int
	var nkeys int
	var rkeys uint64
	var rdratio uint64
	var long bool
	var duration uint64
	var cpuprof string
	var exp bool
	flag.IntVar(&nthrds, "nthrds", 1, "number of threads")
	flag.IntVar(&nkeys, "nkeys", 1, "number of keys accessed per txn")
	flag.Uint64Var(&rkeys, "rkeys", 1000, "access keys within [0:rkeys)")
	flag.Uint64Var(&rdratio, "rdratio", 80, "read ratio")
	flag.BoolVar(&long, "long", false, "background long-running RO transactions")
	flag.Uint64Var(&duration, "duration", 3, "benchmark duration (seconds)")
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

	if long {
		go longReader(txnMgr, rkeys)
	}
	done = false
	for i := 0; i < nthrds; i++ {
		src := rand.NewSource(int64(i))
		go worker(txnMgr, src, chCommitted, chTotal, nkeys, rkeys, rdratio)
	}
	time.Sleep(time.Duration(duration) * time.Second)
	done = true

	var c uint64 = 0
	var t uint64 = 0
	for i := 0; i < nthrds; i++ {
		c += <-chCommitted
		t += <-chTotal
	}
	rate := float64(c) / float64(t)
	tp := float64(c) / float64(duration) / 1000000.0

	if exp {
		fmt.Printf("%d, %d, %d, %d, %d, %d, %f, %f\n",
			nthrds, nkeys, rkeys, duration, c, t, tp, rate)
	} else {
		fmt.Printf("committed / total = %d / %d (%f).\n", c, t, rate)
		fmt.Printf("tp = %f (M txns/s).\n", tp)
	}
}
