// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/codahale/hdrhistogram"
	_ "github.com/lib/pq"
)

var concurrency = flag.Int("concurrency", 2*runtime.NumCPU(), "Number of concurrent writers inserting blocks")
var ddls = flag.String("ddls", "ddls.sql", "which ddls file to load")
var drop = flag.Bool("drop", false, "Drop the database and recreate")
var duration = flag.Duration("duration", 0, "The duration to run. If 0, run forever.")
var load = flag.Bool("load", false, "Generate fresh TPCC data. Use with -drop")
var maxOps = flag.Uint64("max-ops", 0, "Maximum number of operations to run")
var tolerateErrors = flag.Bool("tolerate-errors", false, "Keep running on error")
var verbose = flag.Bool("v", false, "Print verbose debug output")
var warehouses = flag.Int("warehouses", 1, "number of warehouses for loading")

// outputInterval = interval at which information is output to console.
var outputInterval = flag.Duration("output-interval", 1*time.Second, "Interval of output")

const (
	minLatency = 100 * time.Microsecond
	maxLatency = 10 * time.Second
)

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <db URL>\n\n", os.Args[0])
	flag.PrintDefaults()
}

// numOps keeps a global count of successful operations.
var numOps uint64

func setupDatabase(dbURL string) (*sql.DB, error) {
	if *verbose {
		fmt.Printf("connecting to db: %s\n", dbURL)
	}
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}

	// Open connection to server and create a database.
	db, err := sql.Open("postgres", parsedURL.String())
	if err != nil {
		return nil, err
	}

	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(*concurrency + 1)
	db.SetMaxIdleConns(*concurrency + 1)

	return db, nil
}

func main() {
	flag.Usage = usage
	flag.Parse()
	if *verbose {
		fmt.Fprintf(os.Stdout, "Starting TPC-C load generator\n")
	}

	dbURL := "postgresql://root@localhost:26257/tpcc?sslmode=disable"
	if flag.NArg() == 1 {
		dbURL = flag.Arg(0)
	}

	db, err := setupDatabase(dbURL)

	if err != nil {
		fmt.Printf("Setting up database connection failed: %s, continuing assuming database already exists.", err)
	}

	if *drop {
		if _, err := db.Exec("DROP DATABASE tpcc"); err != nil {
			panic(err)
		}
	}

	if *load {
		loadSchema(db)
		generateData(db)
	}

	start := time.Now()
	errCh := make(chan error)
	var wg sync.WaitGroup
	workers := make([]*worker, *concurrency)
	for i := range workers {
		workers[i] = newWorker(db, &wg)
		go workers[i].run(errCh, &wg)
	}

	var numErr int
	tick := time.Tick(*outputInterval)
	done := make(chan os.Signal, 3)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		wg.Wait()
		done <- syscall.Signal(0)
	}()

	if *duration > 0 {
		go func() {
			time.Sleep(*duration)
			done <- syscall.Signal(0)
		}()
	}

	defer func() {
		// Output results that mimic Go's built-in benchmark format.
		elapsed := time.Since(start)
		fmt.Printf("%s\t%8d\t%12.1f ns/op\n",
			"TPCC", numOps, float64(elapsed.Nanoseconds())/float64(numOps))
	}()

	cumLatency := hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	cumLatencyByOp := make(map[txType]*hdrhistogram.Histogram)
	for i := newOrderType; i <= stockLevelType; i++ {
		cumLatencyByOp[i] = hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	}

	lastNow := time.Now()
	var lastOps uint64
	for i := 0; ; {
		select {
		case err := <-errCh:
			numErr++
			if !*tolerateErrors {
				log.Fatal(err)
			} else {
				log.Print(err)
			}
			continue

		case <-tick:
			var h *hdrhistogram.Histogram
			hByOp := make([]*hdrhistogram.Histogram, nTxTypes)
			tmp := make([]*hdrhistogram.Histogram, nTxTypes)
			for _, w := range workers {
				w.latency.Lock()
				m := w.latency.Merge()
				w.latency.Rotate()
				for i, l := range w.latency.byOp {
					tmp[i] = l.Merge()
					l.Rotate()
				}
				w.latency.Unlock()
				if h == nil {
					h = m
				} else {
					h.Merge(m)
				}
				for i, l := range tmp {
					if hByOp[i] == nil {
						hByOp[i] = l
					} else {
						hByOp[i].Merge(l)
					}
				}
			}

			cumLatency.Merge(h)
			p50 := h.ValueAtQuantile(50)
			p95 := h.ValueAtQuantile(95)
			p99 := h.ValueAtQuantile(99)
			pMax := h.ValueAtQuantile(100)

			now := time.Now()
			elapsed := now.Sub(lastNow)
			ops := atomic.LoadUint64(&txs[newOrderType].numOps)
			if i%20 == 0 {
				fmt.Println("_elapsed___tpmC(inst)___tpmC(cum)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			i++
			fmt.Printf("%8s %14.1f %14.1f %8.1f %8.1f %8.1f %8.1f\n",
				time.Duration(time.Since(start).Seconds()+0.5)*time.Second,
				float64(ops-lastOps)/elapsed.Seconds(),
				float64(ops)/time.Since(start).Seconds(),
				time.Duration(p50).Seconds()*1000,
				time.Duration(p95).Seconds()*1000,
				time.Duration(p99).Seconds()*1000,
				time.Duration(pMax).Seconds()*1000)
			lastOps = ops
			lastNow = now

		case <-done:
			for _, w := range workers {
				w.latency.Lock()
				m := w.latency.Merge()
				w.latency.Rotate()
				w.latency.Unlock()
				cumLatency.Merge(m)
			}

			avg := cumLatency.Mean()
			p50 := cumLatency.ValueAtQuantile(50)
			p95 := cumLatency.ValueAtQuantile(95)
			p99 := cumLatency.ValueAtQuantile(99)
			pMax := cumLatency.ValueAtQuantile(100)

			ops := atomic.LoadUint64(&txs[newOrderType].numOps)
			elapsed := time.Since(start).Seconds()
			fmt.Println("\n_elapsed___errors_____ops(total)___tpmC(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			fmt.Printf("%7.1fs %14d %14.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n\n",
				time.Since(start).Seconds(),
				ops, float64(ops)/elapsed,
				time.Duration(avg).Seconds()*1000,
				time.Duration(p50).Seconds()*1000,
				time.Duration(p95).Seconds()*1000,
				time.Duration(p99).Seconds()*1000,
				time.Duration(pMax).Seconds()*1000)
			return
		}
	}
}
