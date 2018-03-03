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

var check = flag.Bool("check", false, "Run consistency checks.")
var concurrency = flag.Int("concurrency", 2*runtime.NumCPU(), "Number of terminals (ignored unless -no-wait is specified)")
var connsPerURL = flag.Int("conns-per-url", 0, "Number of connections to open per url")
var drop = flag.Bool("drop", false, "Drop the database and recreate")
var duration = flag.Duration("duration", 0, "The duration to run. If 0, run forever.")
var interleave = flag.Bool("interleave", false, "Use interleaved data")
var load = flag.Bool("load", false, "Generate fresh TPCC data. Use with -drop")
var loadIndexes = flag.Bool("load-indexes", true, "Load indexes.")
var loadOffset = flag.Int("load-offset", 0, "Warehouse offset for loading")
var loadSchema = flag.Bool("load-schema", true, "Load schema.")
var maxOps = flag.Uint64("max-ops", 0, "Maximum number of operations to run")
var noWait = flag.Bool("no-wait", false, "Run in no wait mode (no think/keying time)")
var opsStats = flag.Bool("ops-stats", false, "Print stats for all operations, not just newOrders")
var scatter = flag.Bool("scatter", false, "Scatter ranges")
var split = flag.Bool("split", false, "Split tables")
var run = flag.Bool("run", true, "Run benchmark")
var ramp = flag.Duration("ramp", 30*time.Second, "The duration over which to ramp up workers")
var tolerateErrors = flag.Bool("tolerate-errors", false, "Keep running on error")
var serializable = flag.Bool("serializable", false, "Force serializable mode")
var verbose = flag.Bool("v", false, "Print verbose debug output")
var warehouses = flag.Int("warehouses", 1, "number of warehouses for loading")

var usePostgres bool
var txOpts *sql.TxOptions

var mix = flag.String("mix", "newOrder=10,payment=10,orderStatus=1,delivery=1,stockLevel=1", "Weights for the transaction mix. The default matches the TPCC spec.")

const (
	minLatency = 100 * time.Microsecond
	maxLatency = 100 * time.Second
)

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <db URL>\n\n", os.Args[0])
	flag.PrintDefaults()
}

// numOps keeps a global count of successful operations.
var numOps uint64

func setupDatabase(parsedURL *url.URL, dbName string, nConns int) (*sql.DB, error) {
	if *verbose {
		fmt.Printf("connecting to db: %s\n", parsedURL)
	}

	parsedURL.Path = dbName

	// Open connection to server and create a database.
	db, err := sql.Open("postgres", parsedURL.String())
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(nConns)
	db.SetMaxIdleConns(nConns)

	return db, nil
}

func main() {
	flag.Usage = usage
	flag.Parse()
	if *verbose {
		fmt.Fprintf(os.Stdout, "Starting TPC-C load generator\n")
	}

	dbName := "tpcc"
	if *interleave {
		dbName = "tpccinterleaved"
	}

	args := flag.Args()
	if len(args) == 0 {
		args = append(args, "postgresql://root@localhost:26257/?sslmode=disable")
	}

	dbURLs := make([]*url.URL, len(args))
	dbs := make([]*sql.DB, len(args))
	for i := range args {
		parsedURL, err := url.Parse(args[i])
		if err != nil {
			fmt.Println("Failed to parse url:", err)
			os.Exit(1)
		}
		dbURLs[i] = parsedURL
	}

	usePostgres = dbURLs[0].Port() == "5432"

	var workers []*worker
	if *noWait {
		workers = make([]*worker, *concurrency)
	} else {
		// 10 workers per warehouse.
		workers = make([]*worker, *warehouses*10)
	}

	if *connsPerURL == 0 {
		*connsPerURL = *warehouses
	}
	for i, dbURL := range dbURLs {
		db, err := setupDatabase(dbURL, dbName, *connsPerURL/len(dbURLs))
		if err != nil {
			fmt.Printf("Setting up database connection to %s failed: %s, continuing assuming database already exists.", dbURL, err)
		}

		dbs[i] = db
	}

	if *serializable {
		txOpts = &sql.TxOptions{Isolation: sql.LevelSerializable}
	}

	// For setup, use the first db in the list.
	db := dbs[0]

	if *drop {
		if usePostgres {
			if _, err := db.Exec("DROP SCHEMA public CASCADE"); err != nil {
				fmt.Println("couldn't drop database:", err)
				os.Exit(1)
			}
		} else {
			if _, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", dbName)); err != nil {
				fmt.Println("couldn't drop database:", err)
				os.Exit(1)
			}
		}
	}

	if *load {
		if *loadSchema {
			if usePostgres {
				if _, err := db.Exec("CREATE SCHEMA IF NOT EXISTS public"); err != nil {
					fmt.Println("couldn't drop database:", err)
					os.Exit(1)
				}
			} else {
				if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + dbName); err != nil {
					fmt.Println("couldn't create database: ", err)
					os.Exit(1)
				}
			}

			doLoadSchema(db, *interleave, false, usePostgres)
		}
		generateData(db, *loadOffset)
	}

	if *loadIndexes {
		doLoadSchema(db, *interleave, true, usePostgres)
	}

	if *split {
		splitTables(db, *warehouses)
	}

	if *scatter {
		scatterRanges(db)
	}

	if *check {
		if err := checkConsistency(db); err != nil {
			fmt.Printf("check consistency failed: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	if !*run {
		os.Exit(0)
	}

	initializeMix()

	start := time.Now()
	errCh := make(chan error)
	var wg sync.WaitGroup
	for i := range workers {
		w := i % *warehouses
		workers[i] = newWorker(i, w, dbs[w%len(dbs)], &wg)
	}

	go func() {
		// Starter the workers evenly spaced over 30s to avoid any thundering
		// behavior.
		sleepTime := *ramp / time.Duration(len(workers))
		for i := range workers {
			go workers[i].run(errCh)
			time.Sleep(sleepTime)
		}
	}()

	var numErr int
	tick := time.Tick(time.Second)
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

	cumLatency := hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	cumLatencyByOp := make([]*hdrhistogram.Histogram, nTxTypes)
	for i := newOrderType; i <= stockLevelType; i++ {
		cumLatencyByOp[i] = hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	}

	lastNow := time.Now()
	var lastOps uint64
	lastOpsByOp := make([]uint64, nTxTypes)
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

			for i, h := range hByOp {
				cumLatencyByOp[i].Merge(h)
			}

			cumLatency.Merge(h)
			now := time.Now()
			elapsed := now.Sub(lastNow)
			ops := numOps
			if i%20 == 0 {
				fmt.Println("_time______opName__ops/s(inst)__ops/s(cum)__p50(ms)__p90(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			i++
			totalTime := time.Duration(time.Since(start).Seconds()+0.5) * time.Second
			if *opsStats {
				fmt.Printf("%5s %11s %12.1f %11.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
					totalTime,
					"all",
					float64(ops-lastOps)/elapsed.Seconds(),
					float64(ops)/time.Since(start).Seconds(),
					time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(90)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(100)).Seconds()*1000)
			}

			for i, h := range hByOp {
				cumLatencyByOp[i].Merge(h)
				numOpsByOp := atomic.LoadUint64(&txs[i].numOps)
				if *opsStats || txType(i) == newOrderType {
					fmt.Printf("%5s %11s %12.1f %11.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
						totalTime,
						txs[i].name,
						float64(numOpsByOp-lastOpsByOp[i])/elapsed.Seconds(),
						float64(numOpsByOp)/time.Since(start).Seconds(),
						time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
						time.Duration(h.ValueAtQuantile(90)).Seconds()*1000,
						time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
						time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
						time.Duration(h.ValueAtQuantile(100)).Seconds()*1000)
				}
				lastOpsByOp[i] = numOpsByOp
			}

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

			fmt.Println("\n_elapsed______opName__ops(total)__ops/m(cum)__avg(ms)__p50(ms)__p90(ms)__p95(ms)__p99(ms)_pMax(ms)")
			totalTime := time.Since(start).Seconds()
			for i, h := range cumLatencyByOp {
				ops := atomic.LoadUint64(&txs[i].numOps)
				fmt.Printf("%7.1fs %11s %11d %11.1f %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
					totalTime,
					txs[i].name,
					ops, float64(ops)/totalTime*60,
					time.Duration(h.Mean()).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(90)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(100)).Seconds()*1000)
			}
			return
		}
	}
}
