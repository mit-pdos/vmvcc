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
	"github.com/mit-pdos/vmvcc/vmvcc"
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

func stockscanner(
	db *vmvcc.DB, nWarehouses uint8, nItems uint32,
	interval time.Duration,
) {
	t := db.NewTxn()

	for !done {
		TxnStockScan(t, nWarehouses, nItems)
		time.Sleep(interval)
	}
}

func worker(
	db *vmvcc.DB, gen *Generator,
	chCommitted, chTotal chan []uint64,
) {
	nCommittedTxns := make([]uint64, 5)
	nTotalTxns := make([]uint64, 5)

	/* Create a new tranasction object. */
	t := db.NewTxn()
	ctx := NewTPCContext()

	/* Start running TPC-C transactions. */
	for !done {
		var ok bool = false
		x := gen.PickTxn()
		switch x {
		case TXN_NEWORDER:
			p := gen.GetNewOrderInput()
			for !ok {
				_, _, _, ok = TxnNewOrder(t, p)
			}
		case TXN_PAYMENT:
			p := gen.GetPaymentInput()
			for !ok {
				ok = TxnPayment(t, p)
			}
		case TXN_ORDERSTATUS:
			p := gen.GetOrderStatusInput()
			for !ok {
				_, ok = TxnOrderStatus(t, p, ctx)
			}
		case TXN_DELIVERY:
			p := gen.GetDeliveryInput()
			for !ok {
				_, ok = TxnDelivery(t, p)
			}
		case TXN_STOCKLEVEL:
			p := gen.GetStockLevelInput()
			for !ok {
				_, ok = TxnStockLevel(t, p)
			}
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
	var stockscan uint64
	var cpuprof string
	var debug bool
	var w workloads = make([]uint64, 0)
	flag.IntVar(&nthrds, "nthrds", 1, "number of threads")
	flag.Var(&w, "workloads", "ratio of each TPC-C transaction")
	flag.Uint64Var(&stockscan, "stockscan", 0, "interval of stock scan transaction (0 to disable)")
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

	db := vmvcc.MkDB()
	db.ActivateGC()

	var nItems uint32 = N_ITEMS
	var nLocalDistricts uint8 = N_DISTRICTS_PER_WAREHOUSE
	var nLocalCustomers uint32 = N_CUSTOMERS_PER_DISTRICT
	var nInitLocalNewOrders uint32 = N_INIT_NEW_ORDERS_PER_DISTRICT

	dprintf(debug, "Loading items...")
	start := time.Now()
	txnitem := db.NewTxn()
	LoadTPCCItems(txnitem, nItems)
	elapsed := time.Since(start)
	dprintf(debug, "Done (%s).", elapsed)

	var wg sync.WaitGroup
	dprintf(debug, "Loading %d warehouses...", nWarehouses)
	start = time.Now()
	for wid := uint8(1); wid <= nWarehouses; wid++ {
		txnwh := db.NewTxn()
		wg.Add(1)
		go func(wid uint8) {
			defer wg.Done()
			LoadOneTPCCWarehouse(
				txnwh, wid,
				nItems, nWarehouses,
				nLocalDistricts, nLocalCustomers, nInitLocalNewOrders,
			)
		}(wid)
	}
	wg.Wait()
	elapsed = time.Since(start)
	dprintf(debug, "Done (%s).", elapsed)

	if stockscan > 0 {
		interval := time.Duration(stockscan) * time.Millisecond
		go stockscanner(db, nWarehouses, nItems, interval)
	}

	dprintf(debug, "Running benchmark...")
	start = time.Now()
	done = false
	for wid := uint8(1); wid <= nWarehouses; wid++ {
		gen := NewGenerator(wid, w, nItems, nWarehouses, nLocalDistricts, nLocalCustomers)
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

	fmt.Printf("%d, %d, %d, %.3f, %.2f\n",
		nthrds, stockscan, duration, tp, rate)
}
