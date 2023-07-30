package main

import (
	"time"
	"fmt"
	"runtime"
	"runtime/pprof"
	"flag"
	"os"
	"log"
	"github.com/mit-pdos/vmvcc/vmvcc"
)

var done, warmup bool
var szrec int = 100

func populateDataBody(txn *vmvcc.Txn, key uint64) bool {
	s := string(make([]byte, szrec))
	txn.Write(key, s)
	return true
}

func populateData(db *vmvcc.DB, rkeys uint64) {
	t := db.NewTxn()
	for k := uint64(0); k < rkeys; k++ {
		body := func(txn *vmvcc.Txn) bool {
			return populateDataBody(txn, k)
		}
		t.Run(body)
	}
}

func longReaderBody(txn *vmvcc.Txn, gen *Generator) bool {
	for i := 0; i < 10000; i++ {
		key := gen.PickKey()
		txn.Read(key)
	}
	return true
}

func longReader(db *vmvcc.DB, gen *Generator) {
	t := db.NewTxn()

	for !done {
		body := func(txn *vmvcc.Txn) bool {
			return longReaderBody(txn, gen)
		}
		t.Run(body)
	}
}

func workerRWBody(txn *vmvcc.Txn, keys []uint64, ops []int, buf []byte) bool {
	for i, k := range(keys) {
		if ops[i] == OP_RD {
			txn.Read(k)
		} else if ops[i] == OP_WR {
			for j := range buf {
				buf[j] = 'b'
			}
			s := string(buf)
			txn.Write(k, s)
		}
	}
	return true
}

func workerRW(
	db *vmvcc.DB, gen *Generator,
	chCommitted, chTotal chan uint64,
) {
	// runtime.LockOSThread()
	var committed uint64 = 0
	var total uint64 = 0
	nKeys := gen.NKeys()

	keys := make([]uint64, nKeys)
	ops := make([]int, nKeys)

	t := db.NewTxn()

	buf := make([]byte, szrec)
	for !done {
		for i := 0; i < nKeys; i++ {
			keys[i] = gen.PickKey()
			ops[i] = gen.PickOp()
		}
		body := func(txn *vmvcc.Txn) bool {
			return workerRWBody(txn, keys, ops, buf)
		}
		ok := t.Run(body)
		if !warmup {
			continue
		}
		if ok {
			committed++
		}
		total++
	}

	chCommitted <-committed
	chTotal <-total
}

func workerScanBody(txn *vmvcc.Txn, key uint64) bool {
	for offset := uint64(0); offset < 100; offset++ {
		txn.Read(key + offset)
	}
	return true
}

func workerScan(
	db *vmvcc.DB, gen *Generator,
	chCommitted, chTotal chan uint64,
) {
	// runtime.LockOSThread()
	var committed uint64 = 0
	var total uint64 = 0

	t := db.NewTxn()

	for !done {
		key := gen.PickKey()
		body := func(txn *vmvcc.Txn) bool {
			return workerScanBody(txn, key)
		}
		ok := t.Run(body)
		if !warmup {
			continue
		}
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
	var heapprof string
	var exp bool
	flag.IntVar(&nthrds, "nthrds", 1, "number of threads")
	flag.IntVar(&nkeys, "nkeys", 1, "number of keys accessed per txn")
	flag.Uint64Var(&rkeys, "rkeys", 1000, "access keys within [0:rkeys)")
	flag.Uint64Var(&rdratio, "rdratio", 80, "read ratio (200 for scan)")
	flag.Float64Var(&theta, "theta", 0.8, "zipfian theta (the higher the more contended; -1 for uniform)")
	flag.BoolVar(&long, "long", false, "background long-running RO transactions")
	flag.Uint64Var(&duration, "duration", 3, "benchmark duration (seconds)")
	flag.StringVar(&cpuprof, "cpuprof", "cpu.prof", "write cpu profile to cpuprof")
	flag.StringVar(&heapprof, "heapprof", "heap.prof", "write heap profile to heapprof")
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

	var nthrdsro int = 8

	gens := make([]*Generator, nthrds + nthrdsro)
	for i := 0; i < nthrds; i++ {
		gens[i] = NewGenerator(i, nkeys,  rkeys, rdratio, theta)
	}
	for i := 0; i < nthrdsro; i++ {
		gens[i + nthrds] = NewGenerator(i + nthrds, nkeys,  rkeys, rdratio, theta)
	}

	db := vmvcc.MkDB()
	populateData(db, rkeys)
	if !exp {
		fmt.Printf("Database populated.\n")
	}

	db.ActivateGC()

	/* Start a long-running reader. */
	if long {
		for i := 0; i < nthrdsro; i++ {
			go longReader(db, gens[nthrds + i])
		}
	}

	done = false
	warmup = false
	for i := 0; i < nthrds; i++ {
		if rdratio == 200 {
			go workerScan(db, gens[i], chCommitted, chTotal)
		} else {
			go workerRW(db, gens[i], chCommitted, chTotal)
		}
	}
	// time.Sleep(time.Duration(60) * time.Second)
	warmup = true
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
		fmt.Printf("%d, %d, %d, %d, %.2f, %v, %d, %f, %f\n",
			nthrds, nkeys, rkeys, rdratio, theta, long, duration, tp, rate)
	} else {
		fmt.Printf("committed / total = %d / %d (%f).\n", c, t, rate)
		fmt.Printf("tp = %f (M txns/s).\n", tp)
	}

	if heapprof != "" {
		f, err := os.Create(heapprof)
		if err != nil {
			log.Fatal("could not create hea[ profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not start heap profile: ", err)
		}
	}
}
