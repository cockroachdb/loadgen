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

	"github.com/cockroachdb/cockroach-go/crdb"
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
	stats         [statsLength]uint64
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
		stats:         [statsLength]uint64{},
	}
}

func (yw *ycsbWorker) hashKey(key []byte, maxValue uint64) uint64 {
	yw.hashFunc.Reset()
	if _, err := yw.hashFunc.Write(key); err != nil {
		panic(err)
	}
	hashedKey := yw.hashFunc.Sum64()
	hashedKeyModMax := hashedKey % maxValue
	return hashedKeyModMax
}

// Keys are chosen by first drawing from a Zipf distribution and hashing the
// drawn value, so that not all hot keys are close together.
// See YCSB paper section 5.3 for a complete description of how keys are chosen.
func (yw *ycsbWorker) nextReadKey() (uint64, error) {
	var hashedKey uint64
	draw := yw.zipfR.Uint64()
	buf := make([]byte, 8)
	binary.PutUvarint(buf, draw)
	hashedKey = yw.hashKey(buf, *maxWrites)
	if *verbose {
		fmt.Printf("reader drew: %d -> %d\n", draw, hashedKey)
	}
	return hashedKey, nil
}

func (yw *ycsbWorker) nextWriteKey() (uint64, error) {
	key, err := yw.zipfR.IncrementIMax()
	if err != nil {
		return key, err
	}
	buf := make([]byte, 8)
	binary.PutUvarint(buf, key)
	hashedKey := yw.hashKey(buf, *maxWrites)
	if *verbose {
		fmt.Printf("writer drew: fnv(%d) -> %d\n", key, hashedKey)
	}
	return hashedKey, nil
}

// runLoader inserts n rows in parallel across numWorkers, with
// row_id = i*numWorkers + thisWorkerNum for i = 0...(n-1)
func (yw *ycsbWorker) runLoader(n int, numWorkers int, thisWorkerNum int, wg *sync.WaitGroup) {
	defer func() {
		if *verbose {
			fmt.Printf("Worker %d done loading\n", yw.workerID)
		}
		wg.Done()
	}()
	if *verbose {
		fmt.Printf("Worker %d loading %d rows of data\n", yw.workerID, n)
	}
	for i := 0; i < n; i++ {
		key := uint64((i * numWorkers) + thisWorkerNum)
		buf := make([]byte, 8)
		binary.PutUvarint(buf, key)
		hashedKey := yw.hashKey(buf, *maxWrites)
		if err := yw.insertRow(hashedKey); err != nil {
			if *verbose {
				fmt.Printf("error loading row %d: %s\n", i, err)
			}
			atomic.AddUint64(&yw.stats[writeErrors], 1)
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
				atomic.AddUint64(&yw.stats[readErrors], 1)
				errCh <- err
			}
		case writeOp:
			if atomic.LoadUint64(&globalStats[writes]) > *maxWrites {
				break
			}
			key, err := yw.nextWriteKey()
			if err != nil {
				errCh <- err
				atomic.AddUint64(&yw.stats[writeErrors], 1)
			}
			if err = yw.insertRow(key); err != nil {
				errCh <- err
				atomic.AddUint64(&yw.stats[writeErrors], 1)
			}
		case scanOp:
			if err := yw.scanRows(); err != nil {
				atomic.AddUint64(&yw.stats[scanErrors], 1)
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

func (yw *ycsbWorker) insertRow(key uint64) error {
	fields := make([]string, numTableFields+1)
	fields[0] = strconv.FormatUint(key, 10)
	for i := 1; i < len(fields); i++ {
		fields[i] = yw.randString(fieldLength)
	}
	values := strings.Join(fields, ", ")
	// TODO(arjun): Consider using a prepared statement here.
	insertString := fmt.Sprintf("INSERT INTO usertable VALUES (%s)", values)
	err := crdb.ExecuteTx(yw.db, func(tx *sql.Tx) error {
		_, inErr := tx.Exec(insertString)
		return inErr
	})
	if err != nil {
		return err
	}

	atomic.AddUint64(&yw.stats[writes], 1)
	return nil
}

func (yw *ycsbWorker) readRow() error {
	key, err := yw.nextReadKey()
	if err != nil {
		atomic.AddUint64(&yw.stats[readErrors], 1)
		return err
	}
	readString := fmt.Sprintf("SELECT * FROM usertable WHERE ycsb_key=%d", key)
	return crdb.ExecuteTx(yw.db, func(tx *sql.Tx) error {
		res, inErr := tx.Query(readString)
		if inErr != nil {
			return inErr
		}
		var rowsFound int
		for res.Next() {
			rowsFound++
		}
		if *verbose {
			fmt.Printf("reader found %d rows for key %d\n",
				rowsFound, key)
		}
		if inErr := res.Close(); err != nil {
			return inErr
		}
		if rowsFound > 0 {
			atomic.AddUint64(&yw.stats[nonEmptyReads], 1)
			return nil
		}
		atomic.AddUint64(&yw.stats[emptyReads], 1)
		return nil
	})
}

func (yw *ycsbWorker) scanRows() error {
	atomic.AddUint64(&yw.stats[scans], 1)
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

	err = crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		_, inErr := db.Exec("CREATE DATABASE IF NOT EXISTS ycsb")
		return inErr
	})
	if err != nil {
		if *verbose {
			fmt.Printf("Failed to create the database, attempting to continue... %s\n",
				err)
		}
	}

	if *drop {
		if *verbose {
			fmt.Println("Dropping the table")
		}
		err = crdb.ExecuteTx(db, func(tx *sql.Tx) error {
			_, inErr := db.Exec("DROP TABLE IF EXISTS usertable")
			return inErr
		})
		if err != nil {
			if *verbose {
				fmt.Printf("Failed to drop the table: %s\n", err)
			}
			return nil, err
		}
	}

	// Create the initial table for storing blocks.
	createStmt := `
	CREATE TABLE IF NOT EXISTS usertable(
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

// Periodically we compute global statistics to print by aggregating all the
// individual worker stats.
func updateGlobalStats(workers []*ycsbWorker) {
	for i := 0; i < int(statsLength); i++ {
		globalStats[i] = 0
	}
	for _, worker := range workers {
		for i := 0; i < int(statsLength); i++ {
			globalStats[i] += worker.stats[i]
		}
	}
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
	start := lastNow
	var opsCount, lastOpsCount uint64
	lastGlobalStats := make([]uint64, len(globalStats))
	workers := make([]*ycsbWorker, *concurrency)

	zipfR, err := NewZipfGenerator(zipfIMin, *initialLoad, zipfS, *verbose)
	if err != nil {
		panic(err)
	}

	loadStart := time.Now()
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		workers[i] = newYcsbWorker(db, zipfR, i, *workload)
		go workers[i].runLoader(int(*initialLoad)/len(workers), len(workers), i, &wg)
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
			elapsed := time.Since(lastNow)

			for j := 0; j < len(globalStats); j++ {
				lastGlobalStats[j] = globalStats[j]
			}
			updateGlobalStats(workers)
			lastOpsCount = opsCount
			opsCount = globalStats[writes] + globalStats[emptyReads] +
				globalStats[nonEmptyReads] + globalStats[scans]
			if i%20 == 0 {
				fmt.Printf("elapsed______ops/sec____emptyReads_nonEmptyReads______writes________scans___readErrors___scanErrors__writeErrors\n")
			}
			fmt.Printf("%7s %9.1f/sec %12d %12d %12d %12d %12d %12d %12d\n",
				time.Duration(time.Since(start).Seconds()+0.5)*time.Second,
				float64(opsCount-lastOpsCount)/elapsed.Seconds(),
				globalStats[emptyReads]-lastGlobalStats[emptyReads],
				globalStats[nonEmptyReads]-lastGlobalStats[nonEmptyReads],
				globalStats[writes]-lastGlobalStats[writes],
				globalStats[scans]-lastGlobalStats[scans],
				globalStats[readErrors]-lastGlobalStats[readErrors],
				globalStats[scanErrors]-lastGlobalStats[scanErrors],
				globalStats[writeErrors]-lastGlobalStats[writeErrors],
			)
			lastOpsCount = opsCount
			lastNow = now
			i++
		case <-done:
			fmt.Printf(" (%d total errors)\n", numErr)
			return
		}
	}
}
