package main

import (
	"time"
	"fmt"
	"runtime/pprof"
	"flag"
	"os"
	"log"
	"sync"
	"strings"
	"strconv"
	"errors"
	"github.com/mit-pdos/go-mvcc/benchmark/tpcc"
	"github.com/mit-pdos/go-mvcc/txn"
)

var done bool

type workloads []uint64

func (w *workloads) String() string {
	return "ratio of each TPC-C transaction"
}

func (w *workloads) Set(s string) error {
	var sum uint64 = 0
	ratios := strings.Split(s, ",")
	for _, ratio := range ratios {
		n, err := strconv.ParseUint(ratio, 10, 64)
		if err != nil {
			return err
		}
		*w = append(*w, n)
		sum += n
	}

	if len(*w) != 5 || sum != 100 {
		return errors.New("workload should contain exactly 5 numbers summing up to 100.")
	}

	return nil
}

func dprintf(debug bool, format string, a ...interface{}) (n int, err error) {
	if debug {
		log.Printf(format, a...)
	}
	return
}

func worker(
	db *txn.TxnMgr, gen *tpcc.Generator,
	chCommitted, chTotal chan []uint64,
) {
	nCommittedTxns := make([]uint64, 5)
	nTotalTxns := make([]uint64, 5)

	/* Create a new tranasction object. */
	t := db.New()

	/* Start running TPC-C transactions. */
	for !done {
		var ok bool
		x := gen.PickTxn()
		switch x {
		case tpcc.TXN_NEWORDER:
			p := gen.GetNewOrderInput()
			_, _, _, ok = tpcc.TxnNewOrder(t, p)
		case tpcc.TXN_PAYMENT:
			p := gen.GetPaymentInput()
			ok = tpcc.TxnPayment(t, p)
		case tpcc.TXN_ORDERSTATUS:
			p := gen.GetOrderStatusInput()
			_, ok = tpcc.TxnOrderStatus(t, p)
		case tpcc.TXN_DELIVERY:
			p := gen.GetDeliveryInput()
			_, ok = tpcc.TxnDelivery(t, p)
		case tpcc.TXN_STOCKLEVEL:
			p := gen.GetStockLevelInput()
			_, ok = tpcc.TxnStockLevel(t, p)
		}

		if ok {
			nCommittedTxns[x]++
		}
		nTotalTxns[x]++
	}
	chCommitted <-nCommittedTxns
	chTotal <-nTotalTxns
}

func main() {
	var nthrds int
	var duration uint64
	var cpuprof string
	var debug bool
	var w workloads = make([]uint64, 0)
	flag.IntVar(&nthrds, "nthrds", 1, "number of threads")
	flag.Var(&w, "workloads", "ratio of each TPC-C transaction")
	flag.Uint64Var(&duration, "duration", 3, "benchmark duration (seconds)")
	flag.StringVar(&cpuprof, "cpuprof", "", "write cpu profile to cpuprof")
	flag.BoolVar(&debug, "debug", true, "print debug info")
	flag.Parse()

	/**
	 * Initialize the number of warehouses to that of threads.
	 */
	if nthrds > 255 {
		log.Fatalf("nthrds = %d > 255.\n", nthrds)
	}
	nWarehouses := uint8(nthrds)
	dprintf(debug, "Number of threads (also warehouses) = %d", nthrds)

	/**
	 * Default TPC-C workload distribution:
	 * NewOrder    (45%)
	 * Payment     (43%)
	 * OrderStatus (4%)
	 * Delivery    (4%)
	 * StockLevel  (4%)
	 */
	if len(w) != 5 {
		w = []uint64{ 45, 43, 4, 4, 4 }
	}
	dprintf(debug, "Workload distribution (NO, P, OS, D, SL) = %v", w)
	dprintf(debug, "")

	chCommitted := make(chan []uint64)
	chTotal := make(chan []uint64)

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

	var nItems uint32 = tpcc.N_ITEMS
	var nLocalDistricts uint8 = tpcc.N_DISTRICTS_PER_WAREHOUSE
	var nLocalCustomers uint32 = tpcc.N_CUSTOMERS_PER_DISTRICT
	var nInitLocalNewOrders uint32 = tpcc.N_INIT_NEW_ORDERS_PER_DISTRICT

	dprintf(debug, "Loading items...")
	start := time.Now()
	txnitem := db.New()
	tpcc.LoadTPCCItems(txnitem, nItems)
	elapsed := time.Since(start)
	dprintf(debug, "Done (%s).", elapsed)

	var wg sync.WaitGroup
	dprintf(debug, "Loading %d warehouses...", nWarehouses)
	start = time.Now()
	for wid := uint8(1); wid <= nWarehouses; wid++ {
		txnwh := db.New()
		wg.Add(1)
		go func(wid uint8) {
			defer wg.Done()
			tpcc.LoadOneTPCCWarehouse(
				txnwh, wid,
				nItems, nWarehouses,
				nLocalDistricts, nLocalCustomers, nInitLocalNewOrders,
			)
		}(wid)
	}
	wg.Wait()
	elapsed = time.Since(start)
	dprintf(debug, "Done (%s).", elapsed)

	dprintf(debug, "Running benchmark...")
	start = time.Now()
	done = false
	for wid := uint8(1); wid <= nWarehouses; wid++ {
		gen := tpcc.NewGenerator(wid, w, nItems, nWarehouses, nLocalDistricts, nLocalCustomers)
		go worker(db, gen, chCommitted, chTotal)
	}
	time.Sleep(time.Duration(duration) * time.Second)
	done = true
	elapsed = time.Since(start)
	dprintf(debug, "Done (%s).", elapsed)
	dprintf(debug, "")

	nCommittedTxns := make([]uint64, 5)
	nTotalTxns := make([]uint64, 5)
	var committed uint64 = 0
	var total uint64 = 0
	for i := 0; i < nthrds; i++ {
		c := <-chCommitted
		t := <-chTotal
		for x := 0; x < 5; x++ {
			nCommittedTxns[x] += c[x]
			nTotalTxns[x] += t[x]
			committed += c[x]
			total += t[x]
		}
	}
	rate := float64(committed) / float64(total) * 100.0
	tp := float64(committed) / float64(duration) / 1000.0

	dprintf(
		debug, "Commit rate (C / T) = %.2f%% (%d / %d).\n",
		rate, committed, total,
	)
	dprintf(
		debug, "\tNewOrder (C / T) = %.2f%% (%d / %d).\n",
		float64(nCommittedTxns[0]) / float64(nTotalTxns[0]) * 100.0,
		nCommittedTxns[0], nTotalTxns[0],
	)
	dprintf(
		debug, "\tPayment (C / T) = %.2f%% (%d / %d).\n",
		float64(nCommittedTxns[1]) / float64(nTotalTxns[1]) * 100.0,
		nCommittedTxns[1], nTotalTxns[1],
	)
	dprintf(
		debug, "\tOrderStatus (C / T) = %.2f%% (%d / %d).\n",
		float64(nCommittedTxns[2]) / float64(nTotalTxns[2]) * 100.0,
		nCommittedTxns[2], nTotalTxns[2],
	)
	dprintf(
		debug, "\tDelivery (C / T) = %.2f%% (%d / %d).\n",
		float64(nCommittedTxns[3]) / float64(nTotalTxns[3]) * 100.0,
		nCommittedTxns[3], nTotalTxns[3],
	)
	dprintf(
		debug, "\tStockLevel (C / T) = %.2f%% (%d / %d).\n",
		float64(nCommittedTxns[4]) / float64(nTotalTxns[4]) * 100.0,
		nCommittedTxns[4], nTotalTxns[4],
	)
	dprintf(debug, "Throughput = %.3f (K txns/s).\n", tp)

	fmt.Printf("%d, %d, %d, %d, %.3f, %.2f\n",
		nthrds, duration, committed, total, tp, rate)
}
