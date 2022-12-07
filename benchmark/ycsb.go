package main

import (
	"time"
	"fmt"
	"runtime/pprof"
	"flag"
	"os"
	"log"
	"github.com/mit-pdos/go-mvcc/benchmark/ycsb"
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

func populateData(db *txn.TxnMgr, rkeys uint64) {
	t := db.New()
	for k := uint64(0); k < rkeys; k++ {
		body := func(txn *txn.Txn) bool {
			return populateDataBody(txn, k)
		}
		t.DoTxn(body)
	}
}

func longReaderBody(txn *txn.Txn, gen *ycsb.Generator) bool {
	for i := 0; i < 10000; i++ {
		key := gen.PickKey()
		txn.Get(key)
	}
	return true
}

func longReader(db *txn.TxnMgr, gen *ycsb.Generator) {
	t := db.New()

	for !done {
		body := func(txn *txn.Txn) bool {
			return longReaderBody(txn, gen)
		}
		t.DoTxn(body)
	}
}

func workerBody(txn *txn.Txn, keys []uint64, ops []int) bool {
	for i, k := range(keys) {
		if ops[i] == ycsb.OP_RD {
			txn.Get(k)
		} else if ops[i] == ycsb.OP_WR {
			/* TODO: Move slice out of the loop. */
			s := string(make([]byte, szrec))
			txn.Put(k, s)
		}
	}
	return true
}

func worker(
	db *txn.TxnMgr, gen *ycsb.Generator,
	chCommitted, chTotal chan uint64,
) {
	var committed uint64 = 0
	var total uint64 = 0
	nKeys := gen.NKeys()

	keys := make([]uint64, nKeys)
	ops := make([]int, nKeys)

	t := db.New()

	for !done {
		for i := 0; i < nKeys; i++ {
			keys[i] = gen.PickKey()
			ops[i] = gen.PickOp()
		}
		body := func(txn *txn.Txn) bool {
			return workerBody(txn, keys, ops)
		}
		ok := t.DoTxn(body)
		if ok {
			committed++
		}
		total++
	}

	chCommitted <-committed
	chTotal <-total
}

func main() {
	var nthrds int
	var nkeys int
	var rkeys uint64
	var rdratio uint64
	var theta float64
	var long bool
	var duration uint64
	var cpuprof string
	var exp bool
	flag.IntVar(&nthrds, "nthrds", 1, "number of threads")
	flag.IntVar(&nkeys, "nkeys", 1, "number of keys accessed per txn")
	flag.Uint64Var(&rkeys, "rkeys", 1000, "access keys within [0:rkeys)")
	flag.Uint64Var(&rdratio, "rdratio", 80, "read ratio")
	flag.Float64Var(&theta, "theta", 0.8, "zipfian theta (the higher the more contended)")
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

	gens := make([]*ycsb.Generator, nthrds)
	for i := 0; i < nthrds; i++ {
		gens[i] = ycsb.NewGenerator(i, nkeys,  rkeys, rdratio, ycsb.DIST_ZIPFIAN, theta)
	}

	db := txn.MkTxnMgr()
	populateData(db, rkeys)
	if !exp {
		fmt.Printf("Database populated.\n")
	}

	db.ActivateGC()

	/* Start a long-running reader. */
	if long {
		gen := ycsb.NewGenerator(nthrds, nkeys,  rkeys, rdratio, ycsb.DIST_ZIPFIAN, theta)
		go longReader(db, gen)
	}

	done = false
	for i := 0; i < nthrds; i++ {
		go worker(db, gens[i], chCommitted, chTotal)
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
