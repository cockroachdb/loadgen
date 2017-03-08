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
//
// Author: Arjun Narayan
//
// The YCSB example program is intended to simulate the workload specified by
// the Yahoo! Cloud Serving Benchmark.
package main

import (
	"database/sql"
	"encoding/binary"
	"flag"
	"fmt"
	"hash"
	"hash/fnv"
	"log"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

// SQL statements
const (
	numTableFields = 10
	fieldLength    = 100 // In characters
)

const (
	zipfS    = 0.99
	zipfIMin = 1
)

var concurrency = flag.Int("concurrency", 2*runtime.NumCPU(),
	"Number of concurrent workers sending read/write requests.")
var workload = flag.String("workload", "B", "workload type. Choose from A-E.")
var tolerateErrors = flag.Bool("tolerate-errors", false,
	"Keep running on error. (default false)")
var duration = flag.Duration("duration", 0,
	"The duration to run. If 0, run forever.")
var verbose = flag.Bool("v", false, "Print *verbose debug output")
var drop = flag.Bool("drop", true,
	"Drop the existing table and recreate it to start from scratch")
var rateLimit = flag.Uint64("rate-limit", 0,
	"Maximum number of operations per second per worker. Set to zero for no rate limit")
var initialLoad = flag.Uint64("initial-load", 10000,
	"Initial number of rows to sequentially insert before beginning Zipfian workload generation")

// 7 days at 5% writes and 30k ops/s
var maxWrites = flag.Uint64("max-writes", 7*24*3600*1500,
	"Maximum number of writes to perform before halting. This is required for accurately generating keys that are uniformly distributed across the keyspace.")

var splits = flag.Int("splits", 0, "Number of splits to perform before starting normal operations")

// ycsbWorker independently issues reads, writes, and scans against the database.
type ycsbWorker struct {
	workerID int
	db       *sql.DB
	// An RNG used to generate random keys
	zipfR *ZipfGenerator
	// An RNG used to generate random strings for the values
	r             *rand.Rand
	readFreq      float32
	writeFreq     float32
	scanFreq      float32
	minNanosPerOp time.Duration
	hashFunc      hash.Hash64
	hashBuf       [8]byte
}

type statistic int

const (
	nonEmptyReads statistic = iota
	emptyReads
	writes
	scans
	writeErrors
	readErrors
	scanErrors
	statsLength
)

var globalStats [statsLength]uint64

type operation int

const (
	writeOp operation = iota
	readOp
	scanOp
)

func newYcsbWorker(db *sql.DB, zipfR *ZipfGenerator, id int, workloadFlag string) *ycsbWorker {
	if *verbose {
		fmt.Printf("Creating YCSB Worker '%d' running Workload %s\n", id, workloadFlag)
	}
	source := rand.NewSource(int64(time.Now().UnixNano()))
	var readFreq, writeFreq, scanFreq float32

	// TODO(arjun): This could be implemented as a token bucket.
	var minNanosPerOp time.Duration
	if *rateLimit != 0 {
		minNanosPerOp = time.Duration(1000000000 / *rateLimit)
	}
	switch workloadFlag {
	case "A", "a":
		readFreq = 0.5
		writeFreq = 0.5
	case "B", "b":
		readFreq = 0.95
		writeFreq = 0.05
	case "C", "c":
		readFreq = 1.0
	case "D", "d":
		readFreq = 0.95
		writeFreq = 0.95
		panic("Workload D not implemented yet")
		// TODO(arjun) workload D (read latest) requires modifying the RNG to
		// skew to the latest keys, so not done yet.
	case "E", "e":
		scanFreq = 0.95
		writeFreq = 0.05
		panic("Workload E (scans) not implemented yet")
	}
	r := rand.New(source)
	return &ycsbWorker{
		db:            db,
		workerID:      id,
		r:             r,
		zipfR:         zipfR,
		readFreq:      readFreq,
		writeFreq:     writeFreq,
		scanFreq:      scanFreq,
		minNanosPerOp: minNanosPerOp,
		hashFunc:      fnv.New64(),
	}
}

func (yw *ycsbWorker) hashKey(key, maxValue uint64) uint64 {
	yw.hashBuf = [8]byte{} // clear hashBuf
	binary.PutUvarint(yw.hashBuf[:], key)
	yw.hashFunc.Reset()
	if _, err := yw.hashFunc.Write(yw.hashBuf[:]); err != nil {
		panic(err)
	}
	hashedKey := yw.hashFunc.Sum64()
	hashedKeyModMax := hashedKey % maxValue
	return hashedKeyModMax
}

// Keys are chosen by first drawing from a Zipf distribution and hashing the
// drawn value, so that not all hot keys are close together.
// See YCSB paper section 5.3 for a complete description of how keys are chosen.
func (yw *ycsbWorker) nextReadKey() uint64 {
	var hashedKey uint64
	key := yw.zipfR.Uint64()
	hashedKey = yw.hashKey(key, *maxWrites)
	if *verbose {
		fmt.Printf("reader: %d -> %d\n", key, hashedKey)
	}
	return hashedKey
}

func (yw *ycsbWorker) nextWriteKey() uint64 {
	key := yw.zipfR.IMaxHead()
	hashedKey := yw.hashKey(key, *maxWrites)
	if *verbose {
		fmt.Printf("writer: fnv(%d) -> %d\n", key, hashedKey)
	}
	return hashedKey
}

// runLoader inserts n rows in parallel across numWorkers, with
// row_id = i*numWorkers + thisWorkerNum for i = 0...(n-1)
func (yw *ycsbWorker) runLoader(n uint64, numWorkers int, thisWorkerNum int, wg *sync.WaitGroup) {
	defer func() {
		if *verbose {
			fmt.Printf("Worker %d done loading\n", yw.workerID)
		}
		wg.Done()
	}()
	if *verbose {
		fmt.Printf("Worker %d loading %d rows of data\n", yw.workerID, n)
	}
	for i := uint64(thisWorkerNum + zipfIMin); i < n; i += uint64(numWorkers) {
		hashedKey := yw.hashKey(i, *maxWrites)
		if err := yw.insertRow(hashedKey, false); err != nil {
			if *verbose {
				fmt.Printf("error loading row %d: %s\n", i, err)
			}
			atomic.AddUint64(&globalStats[writeErrors], 1)
		} else if *verbose {
			fmt.Printf("loaded %d\n", hashedKey)
		}
	}
}

// runWorker is an infinite loop in which the ycsbWorker reads and writes
// random data into the table in proportion to the op frequencies.
func (yw *ycsbWorker) runWorker(errCh chan<- error, wg *sync.WaitGroup) {
	defer func() {
		if *verbose {
			fmt.Printf("Shutting down worker '%d'\n", yw.workerID)
		}
		wg.Done()
	}()

	for {
		tStart := time.Now()
		switch yw.chooseOp() {
		case readOp:
			if err := yw.readRow(); err != nil {
				atomic.AddUint64(&globalStats[readErrors], 1)
				errCh <- err
			}
		case writeOp:
			if atomic.LoadUint64(&globalStats[writes]) > *maxWrites {
				break
			}
			key := yw.nextWriteKey()
			if err := yw.insertRow(key, true); err != nil {
				errCh <- err
				atomic.AddUint64(&globalStats[writeErrors], 1)
			}
		case scanOp:
			if err := yw.scanRows(); err != nil {
				atomic.AddUint64(&globalStats[scanErrors], 1)
				errCh <- err
			}
		}

		// If we are done faster than the rate limit, wait.
		tElapsed := time.Since(tStart)
		if tElapsed < yw.minNanosPerOp {
			time.Sleep(time.Duration(yw.minNanosPerOp - tElapsed))
		}
	}
}

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// Gnerate a random string of alphabetic characters.
func (yw *ycsbWorker) randString(length int) string {
	str := make([]byte, length)
	for i := range str {
		str[i] = letters[yw.r.Intn(len(letters))]
	}
	return fmt.Sprintf("'%s'", string(str))
}

func (yw *ycsbWorker) insertRow(key uint64, increment bool) error {
	fields := make([]string, numTableFields+1)
	fields[0] = strconv.FormatUint(key, 10)
	for i := 1; i < len(fields); i++ {
		fields[i] = yw.randString(fieldLength)
	}
	values := strings.Join(fields, ", ")
	// TODO(arjun): Consider using a prepared statement here.
	insertString := fmt.Sprintf("INSERT INTO ycsb.usertable VALUES (%s)", values)
	_, err := yw.db.Exec(insertString)
	if err != nil {
		return err
	}

	if increment {
		if err = yw.zipfR.IncrementIMax(); err != nil {
			return err
		}
	}
	atomic.AddUint64(&globalStats[writes], 1)
	return nil
}

func (yw *ycsbWorker) readRow() error {
	key := yw.nextReadKey()
	readString := fmt.Sprintf("SELECT * FROM ycsb.usertable WHERE ycsb_key=%d", key)
	res, err := yw.db.Query(readString)
	if err != nil {
		return err
	}
	var rowsFound int
	for res.Next() {
		rowsFound++
	}
	if *verbose {
		fmt.Printf("reader found %d rows for key %d\n", rowsFound, key)
	}
	if err := res.Close(); err != nil {
		return err
	}
	if rowsFound > 0 {
		atomic.AddUint64(&globalStats[nonEmptyReads], 1)
		return nil
	}
	atomic.AddUint64(&globalStats[emptyReads], 1)
	return nil
}

func (yw *ycsbWorker) scanRows() error {
	atomic.AddUint64(&globalStats[scans], 1)
	return errors.Errorf("not implemented yet")
}

// Choose an operation in proportion to the frequencies.
func (yw *ycsbWorker) chooseOp() operation {
	p := yw.r.Float32()
	if p <= yw.readFreq {
		return readOp
	}
	p -= yw.readFreq
	if p <= yw.writeFreq {
		return writeOp
	}
	p -= yw.writeFreq
	return scanOp
}

// setupDatabase performs initial setup for the example, creating a database
// with a single table. If the desired table already exists on the cluster, the
// existing table will be dropped if the -drop flag was specified.
func setupDatabase(dbURL string) (*sql.DB, error) {
	if *verbose {
		fmt.Printf("Setting up the database. Connecting to db: %s\n", dbURL)
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

	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS ycsb"); err != nil {
		if *verbose {
			fmt.Printf("Failed to create the database, attempting to continue... %s\n",
				err)
		}
	}

	if *drop {
		if *verbose {
			fmt.Println("Dropping the table")
		}
		if _, err := db.Exec("DROP TABLE IF EXISTS ycsb.usertable"); err != nil {
			if *verbose {
				fmt.Printf("Failed to drop the table: %s\n", err)
			}
			return nil, err
		}
	}

	// Create the initial table for storing blocks.
	createStmt := `
CREATE TABLE IF NOT EXISTS ycsb.usertable (
	ycsb_key INT PRIMARY KEY NOT NULL,
	FIELD1 TEXT, 
	FIELD2 TEXT, 
	FIELD3 TEXT, 
	FIELD4 TEXT, 
	FIELD5 TEXT,
	FIELD6 TEXT, 
	FIELD7 TEXT, 
	FIELD8 TEXT, 
	FIELD9 TEXT, 
	FIELD10 TEXT
)`
	if _, err := db.Exec(createStmt); err != nil {
		return nil, err
	}

	return db, nil
}

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <db URL>\n\n", os.Args[0])
	flag.PrintDefaults()
}

func snapshotStats() (s [statsLength]uint64) {
	for i := 0; i < int(statsLength); i++ {
		s[i] = atomic.LoadUint64(&globalStats[i])
	}
	return s
}

func main() {
	flag.Usage = usage
	flag.Parse()
	if *verbose {
		fmt.Fprintf(os.Stdout, "Starting YCSB load generator\n")
	}

	dbURL := "postgresql://root@localhost:26257/ycsb?sslmode=disable"
	if flag.NArg() == 1 {
		dbURL = flag.Arg(0)
	}

	if *concurrency < 1 {
		log.Fatalf("Value of 'concurrency' flag (%d) must be greater than or equal to 1",
			concurrency)
	}

	db, err := setupDatabase(dbURL)

	if err != nil {
		log.Fatalf("Setting up database failed: %s", err)
	}
	if *verbose {
		fmt.Printf("Database setup complete. Loading...\n")
	}

	lastNow := time.Now()
	var lastOpsCount uint64
	var lastStats [statsLength]uint64

	zipfR, err := NewZipfGenerator(zipfIMin, *initialLoad, zipfS, *verbose)
	if err != nil {
		panic(err)
	}

	workers := make([]*ycsbWorker, *concurrency)
	for i := range workers {
		workers[i] = newYcsbWorker(db, zipfR, i, *workload)
	}

	if *splits > 0 {
		for i := 0; i < *splits; i++ {
			key := workers[0].hashKey(uint64(i), *maxWrites)
			if _, err := db.Exec(`ALTER TABLE ycsb.usertable SPLIT AT ($1)`, key); err != nil {
				log.Fatal(err)
			}
		}
	}

	loadStart := time.Now()
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go workers[i].runLoader(*initialLoad, len(workers), i, &wg)
	}
	wg.Wait()
	if *verbose {
		fmt.Printf("Loading complete, total time elapsed: %s\n",
			time.Since(loadStart))
	}

	wg = sync.WaitGroup{}
	errCh := make(chan error)

	for i := range workers {
		wg.Add(1)
		go workers[i].runWorker(errCh, &wg)
	}

	var numErr int
	tick := time.Tick(1 * time.Second)
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

	start := time.Now()
	startOpsCount := globalStats[writes] + globalStats[emptyReads] +
		globalStats[nonEmptyReads] + globalStats[scans]

	for i := 0; ; {
		select {
		case err := <-errCh:
			numErr++
			if !*tolerateErrors {
				log.Fatal(err)
			} else if *verbose {
				log.Print(err)
			}
			continue

		case <-tick:
			now := time.Now()
			elapsed := now.Sub(lastNow)

			stats := snapshotStats()
			opsCount := stats[writes] + stats[emptyReads] +
				stats[nonEmptyReads] + stats[scans]
			if i%20 == 0 {
				fmt.Printf("elapsed______ops/sec__reads/empty/errors___writes/errors____scans/errors\n")
			}
			fmt.Printf("%7s %12.1f %19s %15s %15s\n",
				time.Duration(time.Since(start).Seconds()+0.5)*time.Second,
				float64(opsCount-lastOpsCount)/elapsed.Seconds(),
				fmt.Sprintf("%d / %d / %d",
					stats[nonEmptyReads]-lastStats[nonEmptyReads],
					stats[emptyReads]-lastStats[emptyReads],
					stats[readErrors]-lastStats[readErrors]),
				fmt.Sprintf("%d / %d",
					stats[writes]-lastStats[writes],
					stats[writeErrors]-lastStats[writeErrors]),
				fmt.Sprintf("%d / %d",
					stats[scans]-lastStats[scans],
					stats[scanErrors]-lastStats[scanErrors]))
			lastStats = stats
			lastOpsCount = opsCount
			lastNow = now
			i++

		case <-done:
			stats := snapshotStats()
			opsCount := stats[writes] + stats[emptyReads] +
				stats[nonEmptyReads] + stats[scans] - startOpsCount
			elapsed := time.Since(start).Seconds()
			fmt.Printf("\nelapsed__ops/sec(total)__errors(total)\n")
			fmt.Printf("%6.1fs %14.1f %14d\n",
				time.Since(start).Seconds(),
				float64(opsCount)/elapsed, numErr)
			return
		}
	}
}
