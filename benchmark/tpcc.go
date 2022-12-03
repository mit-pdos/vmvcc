package main

import (
	"time"
	"fmt"
	"runtime/pprof"
	"flag"
	"os"
	"log"
	"sync"
	"math/rand"
	"github.com/mit-pdos/go-mvcc/benchmark/tpcc"
	"github.com/mit-pdos/go-mvcc/txn"
)

var done bool

func dprintf(debug bool, format string, a ...interface{}) (n int, err error) {
	if debug {
		log.Printf(format, a...)
	}
	return
}

func worker(
	db *txn.TxnMgr, gen *tpcc.Generator,
	chCommitted, chTotal chan uint64,
) {
	var committed uint64 = 0
	var total uint64 = 0
	t := db.New()
	for !done {
		p := gen.GetPaymentInput()
		ok := tpcc.TxnPayment(t, p)
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
	var duration uint64
	var cpuprof string
	var debug bool
	flag.IntVar(&nthrds, "nthrds", 1, "number of threads")
	flag.Uint64Var(&duration, "duration", 3, "benchmark duration (seconds)")
	flag.StringVar(&cpuprof, "cpuprof", "cpu.prof", "write cpu profile to cpuprof")
	flag.BoolVar(&debug, "debug", true, "print debug info")
	flag.Parse()

	if nthrds > 255 {
		log.Fatalf("nthrds = %d > 255.\n", nthrds)
	}
	nWarehouses := uint8(nthrds)

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

	db := txn.MkTxnMgr()
	db.ActivateGC()

	var nItems uint32 = 100
	var nLocalDistricts uint8 = 2
	var nLocalCustomers uint32 = 10
	var nInitLocalNewOrders uint32 = 5

	dprintf(debug, "Loading items...")
	txnitem := db.New()
	tpcc.LoadTPCCItems(txnitem, nItems)
	dprintf(debug, "done\n")

	var wg sync.WaitGroup
	dprintf(debug, "Loading %d warehouses...", nWarehouses)
	for wid := uint8(1); wid <= nWarehouses; wid++ {
		txnwh := db.New()
		wg.Add(1)
		go func() {
			defer wg.Done()
			tpcc.LoadOneTPCCWarehouse(
				txnwh, wid,
				nItems, nWarehouses,
				nLocalDistricts, nLocalCustomers, nInitLocalNewOrders,
			)
		}()
	}
	wg.Wait()
	dprintf(debug, "done\n")

	dprintf(debug, "Running benchmark...")
	done = false
	for wid := uint8(1); wid <= nWarehouses; wid++ {
		src := rand.NewSource(int64(wid))
		gen := tpcc.NewGenerator(src, wid, nItems, nWarehouses, nLocalDistricts, nLocalCustomers)
		go worker(db, gen, chCommitted, chTotal)
	}
	time.Sleep(time.Duration(duration) * time.Second)
	done = true
	dprintf(debug, "done\n")

	var c uint64 = 0
	var t uint64 = 0
	for i := 0; i < nthrds; i++ {
		c += <-chCommitted
		t += <-chTotal
	}
	rate := float64(c) / float64(t)
	tp := float64(c) / float64(duration) / 1000000.0

	dprintf(debug, "committed / total = %d / %d (%f).\n", c, t, rate)
	dprintf(debug, "tp = %f (M txns/s).\n", tp)

	fmt.Printf("%d, %d, %d, %d, %f, %f\n",
		nthrds, duration, c, t, tp, rate)
}

