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
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"bytes"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"

	"encoding/binary"

	"hash/fnv"

	"hash"

	"github.com/cockroachdb/cockroach-go/crdb"
	_ "github.com/lib/pq"
)

// SQL statements
const (
	insertStmt = `INSERT INTO %s VALUES (%s)`
	selectStmt = `SELECT * FROM %s WHERE YCSB_KEY=%d`
	createStmt = `CREATE TABLE IF NOT EXISTS %s(
	YCSB_KEY INT PRIMARY KEY NOT NULL,
	FIELD1 TEXT, FIELD2 TEXT, FIELD3 TEXT, FIELD4 TEXT, FIELD5 TEXT,
	FIELD6 TEXT, FIELD7 TEXT, FIELD8 TEXT, FIELD9 TEXT, FIELD10 TEXT)`
	createWithFamilyStmt = `CREATE TABLE IF NOT EXISTS %s(
	YCSB_KEY INT PRIMARY KEY NOT NULL,
	FIELD1 TEXT, FIELD2 TEXT, FIELD3 TEXT, FIELD4 TEXT, FIELD5 TEXT,
	FIELD6 TEXT, FIELD7 TEXT, FIELD8 TEXT, FIELD9 TEXT, FIELD10 TEXT,
	FAMILY f1(ycsb_key, field1, field2, field3, field4, field5,
	field6, field7, field8, field9, field10));`
	dropStmt    = `DROP TABLE IF EXISTS %s`
	numFields   = 10
	fieldLength = 100 // In characters
)

// Constants for the Zipfian RNG
// The following constants use the definitions from [1]: "Quickly Generating Billion-Record Synthetic Databases" by Gray et al, SIGMOD 1994. The values are set to match the values in [2]: https://github.com/brianfrankcooper/YCSB/blob/0a330744c5b6f3b1a49e8988ccf1f6ac748340f5/core/src/main/java/com/yahoo/ycsb/generator/ZipfianGenerator.java
// The following comment translates the names between the two citations.
const (
	// zipfS from [2] = -theta from [1].
	// zipfS = 0.99 TODO(arjun): Golang doesn't support s <= 1, so set to 1.01 for now. Slight skew in distribution results. In the long run we need to write our own incrementing Zipfian generator.
	zipfS = 1.01
	// zipfV = 1, ensures values are 1 indexed.
	zipfV = 1
	// zipfIMax from [2] = N from [1], is incremented with each insert.
)

var concurrency = flag.Int("concurrency", 1,
	"Number of concurrent workers sending read/write requests.")
var workload = flag.String("workload", "B", "workload type. Choose from A-E.")
var tolerateErrors = flag.Bool("tolerate-errors", false,
	"Keep running on error. (default false)")
var noColumnFamilies = flag.Bool("without-column-families", false, "Create a table without explicitly setting column families. Note that if the table already exists in the database this flag has no effect as the existing table's schema is used.")
var tableName = flag.String("tablename", "USERTABLE", "the table name to use.")
var outputInterval = flag.Duration("output-interval", 1*time.Second,
	"Interval of output.")
var duration = flag.Duration("duration", 0,
	"The duration to run. If 0, run forever.")
var benchmarkName = flag.String("benchmark-name", "BenchmarkYCSB",
	"Test name to report for Go benchmark results.")
var verbose = flag.Bool("v", false, "Print *verbose debug output")
var drop = flag.Bool("drop", false,
	"Drop the existing table and recreate it to start from scratch")
var createDB = flag.Bool("create", false,
	"Create the database if it doesn't exist. Syntax unsupported by Postgres")
var rateLimit = flag.Uint64("rate-limit", 1,
	"Maximum number of operations per second per worker. Set to zero for no rate limit")
var initialLoad = flag.Uint64("initial-load", 1000,
	"Initial number of rows to sequentially insert before beginning Zipfian workload generation")

// 4 hours at 5% writes and 30k ops/s
var maxWrites = flag.Uint64("max-writes", 1500*3600*4,
	"Maximum number of writes to perform before halting. This is required for accurately generating keys that are uniformly distributed across the keyspace.")

// YCSBWorker independently issues reads and writes against the database.
type YCSBWorker struct {
	workerID string
	db       *sql.DB
	table    string
	// An RNG used to generate random keys
	zipfR *rand.Zipf
	// An RNG used to generate random strings for the values
	r             *rand.Rand
	readFreq      float32
	writeFreq     float32
	scanFreq      float32
	minNanosPerOp time.Duration
	hashFunc      hash.Hash64
	statsMu       *sync.Mutex
	stats         []uint64
}

const (
	nonEmptyReads = iota
	emptyReads
	writes
	scans
	writeErrors
	readErrors
	scanErrors
	statsLength
)

var globalStatsMu sync.Mutex
var globalStats []uint64

const (
	noOp = iota
	writeOp
	readOp
	scanOp
)

func opToString(op int) string {
	switch op {
	case noOp:
		return "noOp"
	case writeOp:
		return "writeOp"
	case readOp:
		return "readOp"
	case scanOp:
		return "scanOp"
	default:
		return "invalidOp"
	}
}

func newYCSBWorker(db *sql.DB, workloadFlag string) YCSBWorker {
	id := uuid.NewV4().String()
	if *verbose {
		fmt.Printf("Creating YCSB Worker '%s' running Workload %s\n", id, workloadFlag)
	}
	source := rand.NewSource(int64(time.Now().UnixNano()))
	var readFreq, writeFreq, scanFreq float32
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
		// TODO(arjun) workload D (read latest) requires modifying the RNG to skew to the latest keys, so not done yet.
	case "E", "e":
		scanFreq = 0.95
		writeFreq = 0.05
		panic("Workload E (scans) not implemented yet")
	}
	r := rand.New(source)
	zipfR := rand.NewZipf(r, zipfS, zipfV, *maxWrites)
	return YCSBWorker{
		db:            db,
		workerID:      id,
		r:             r,
		zipfR:         zipfR,
		readFreq:      readFreq,
		writeFreq:     writeFreq,
		scanFreq:      scanFreq,
		table:         *tableName,
		minNanosPerOp: minNanosPerOp,
		hashFunc:      fnv.New64(),
		stats:         make([]uint64, statsLength),
		statsMu:       &sync.Mutex{},
	}
}

// We use the FNV hash function to hash keys as it is fast and we don't need
// cryptographically-safe hashing.
func (yw YCSBWorker) hashKey(key []byte, maxValue uint64) uint64 {
	yw.hashFunc.Reset()
	if _, err := yw.hashFunc.Write(key); err != nil {
		panic(err)
	}
	hashedKey := yw.hashFunc.Sum64()
	hashedKeyModMax := hashedKey % *maxWrites
	return hashedKeyModMax
}

// Keys are chosen by first drawing from a Zipf distribution and hashing the
// drawn value, so that not all hot keys are close together.
// See YCSB paper section 5.3 for a complete description of how keys are chosen.
func (yw YCSBWorker) nextReadKey() uint64 {
	var draw, hashedKey uint64
	draw = yw.zipfR.Uint64()
	buf := make([]byte, 8)
	binary.PutUvarint(buf, draw)
	hashedKey = yw.hashKey(buf, *maxWrites)
	binary.PutUvarint(buf, hashedKey)
	if *verbose {
		fmt.Printf("reader drew: %d -> %d\n", draw, hashedKey)
	}
	return hashedKey
}

// TODO(arjun): once we have the incrementing Zipf RNG implemented, we can
// adjust the key selection to the correct distribution. Currently the keys are
// incorrectly skewed, and don't meet the YCSB specification.
func (yw YCSBWorker) nextWriteKey() uint64 {
	draw := yw.zipfR.Uint64()
	buf := make([]byte, 8)
	binary.PutUvarint(buf, draw)
	hashedKey := yw.hashKey(buf, *maxWrites)
	binary.PutUvarint(buf, hashedKey)
	if *verbose {
		fmt.Printf("writer drew: %d -> %d\n", draw, hashedKey)
	}
	return hashedKey
}

// runLoader inserts n rows in parallel across numWorkers, with
// row_id = i*numWorkers + thisWorkerNum for i = 0...(n-1)
func (yw YCSBWorker) runLoader(n int, numWorkers int, thisWorkerNum int, wg *sync.WaitGroup) {
	defer func() {
		if *verbose {
			fmt.Printf("Worker %s done loading\n", yw.workerID)
		}
		wg.Done()
	}()
	if *verbose {
		fmt.Printf("Worker %s loading %d rows of data\n", yw.workerID, n)
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

// runWorker is an infinite loop in which the YCSBWorker reads and writes
// random data into the table in proportion to the op frequencies.
func (yw YCSBWorker) runWorker(errCh chan<- error, wg *sync.WaitGroup) {
	defer func() {
		if *verbose {
			fmt.Printf("Shutting down worker '%s'\n", yw.workerID)
		}
		wg.Done()
	}()

	for {
		tStart := timeutil.Now()
		switch yw.chooseOp() {
		case readOp:
			if err := yw.readRow(); err != nil {
				yw.statsMu.Lock()
				yw.stats[readErrors]++
				yw.statsMu.Unlock()
				errCh <- err
			}
		case writeOp:
			globalStatsMu.Lock()
			if globalStats[writes] > *maxWrites {
				globalStatsMu.Unlock()
				break
			}
			globalStatsMu.Unlock()
			key := yw.nextWriteKey()
			if err := yw.insertRow(key); err != nil {
				errCh <- err
				atomic.AddUint64(&yw.stats[writeErrors], 1)
			}
		case scanOp:
			if err := yw.scanRows(); err != nil {
				yw.statsMu.Lock()
				yw.stats[scanErrors]++
				yw.statsMu.Unlock()
				errCh <- err
			}

		}

		// If we are done faster than the rate limit, wait.
		tElapsed := timeutil.Since(tStart)
		if tElapsed < yw.minNanosPerOp {
			select {
			case <-time.After(time.Duration(yw.minNanosPerOp - tElapsed)):
			}
		}
	}
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// Gnerate a random string of alphabetic characters.
func (yw YCSBWorker) randString(length int) string {
	str := make([]rune, length)
	for i := range str {
		str[i] = letters[yw.r.Intn(len(letters))]
	}
	return string(str)
}

func (yw YCSBWorker) insertRow(key uint64) error {
	fields := make([][]byte, numFields)
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%d, ", key))
	for i := range fields {
		fields[i] = make([]byte, fieldLength)
		str := yw.randString(fieldLength)
		buffer.WriteString(fmt.Sprintf("'%s'", str))
		if i < (len(fields) - 1) {
			buffer.WriteString(", ")
		}
	}

	insertString := fmt.Sprintf(insertStmt, yw.table, buffer.String())
	err := crdb.ExecuteTx(yw.db, func(tx *sql.Tx) error {
		if _, err := tx.Exec(insertString); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	atomic.AddUint64(&yw.stats[writes], 1)
	return nil
}

func (yw YCSBWorker) readRow() error {
	key := yw.nextReadKey()
	readString := fmt.Sprintf(selectStmt, yw.table, key)
	err := crdb.ExecuteTx(yw.db, func(tx *sql.Tx) error {
		res, err := tx.Query(readString)
		if err != nil {
			return err
		}
		var rowsFound int
		for res.Next() {
			rowsFound++
		}
		if *verbose {
			fmt.Printf("reader found %d rows for key %d\n",
				rowsFound, key)
		}
		if err := res.Close(); err != nil {
			return err
		}
		if rowsFound > 0 {
			atomic.AddUint64(&yw.stats[nonEmptyReads], 1)
			return nil
		}
		atomic.AddUint64(&yw.stats[emptyReads], 1)
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (yw YCSBWorker) scanRows() error {
	atomic.AddUint64(&yw.stats[scans], 1)
	return errors.Errorf("Not implemented yet")
}

// Choose an operation in proportion to the frequencies.
func (yw YCSBWorker) chooseOp() int {
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
func setupDatabase(dbURL string, noColFamilies bool) (*sql.DB, error) {
	if *verbose {
		fmt.Printf("Setting up the database. Connecting to db: %s\n", dbURL)
	}
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}
	//parsedURL.Path = "ycsb"

	// Open connection to server and create a database.
	db, err := sql.Open("postgres", parsedURL.String())
	if err != nil {
		return nil, err
	}

	if *createDB {
		err = crdb.ExecuteTx(db, func(tx *sql.Tx) error {
			if _, inErr := db.Exec("CREATE DATABASE ycsb"); inErr != nil {
				return inErr
			}
			return nil
		})
		if err != nil {
			if *verbose {
				fmt.Printf("Failed to create the database, continuing under the assumption that it already exists... %s\n", err)
			}
		}
	}

	if *drop {
		if *verbose {
			fmt.Println("Dropping the database")
		}
		err = crdb.ExecuteTx(db, func(tx *sql.Tx) error {
			if _, inErr := db.Exec(fmt.Sprintf(dropStmt, "usertable")); inErr != nil {
				return inErr
			}
			return nil
		})
		if err != nil {
			if *verbose {
				fmt.Printf("Failed to drop the database: %s\n", err)
			}
			return nil, err
		}
	}

	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(*concurrency + 1)
	db.SetMaxIdleConns(*concurrency + 1)

	create := createWithFamilyStmt
	if noColFamilies {
		create = createStmt
	}
	// Create the initial table for storing blocks.
	stmt := fmt.Sprintf(create, "USERTABLE")
	if *verbose {
		fmt.Println(stmt)
	}
	if _, err := db.Exec(stmt); err != nil {
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
func updateGlobalStatsLocked(workers []YCSBWorker) {
	for i := 0; i < statsLength; i++ {
		globalStats[i] = 0
	}
	for _, worker := range workers {
		worker.statsMu.Lock()
		for i := 0; i < statsLength; i++ {
			globalStats[i] += worker.stats[i]
		}
		worker.statsMu.Unlock()
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
		log.Fatalf("Value of 'concurrency' flag (%d) must be greater than or equal to 1", concurrency)
	}

	globalStats = make([]uint64, statsLength)
	var db *sql.DB
	{
		var err error
		db, err = setupDatabase(dbURL, *noColumnFamilies)

		if err != nil {
			log.Fatalf("Setting up database failed: %s\n", err)
		}
		if *verbose {
			fmt.Printf("Database setup complete. Loading...\n")
		}
	}

	lastNow := time.Now()
	start := lastNow
	var opsCount, lastOpsCount uint64
	lastGlobalStats := make([]uint64, len(globalStats))
	workers := make([]YCSBWorker, *concurrency)

	loadStart := timeutil.Now()
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		workers[i] = newYCSBWorker(db, *workload)
		go workers[i].runLoader(int(*initialLoad)/len(workers), len(workers), i, &wg)
	}
	wg.Wait()
	if *verbose {
		fmt.Printf("Loading complete, total time elapsed: %s\n",
			timeutil.Since(loadStart))
	}

	wg = sync.WaitGroup{}
	errCh := make(chan error)

	for i := range workers {
		wg.Add(1)
		go workers[i].runWorker(errCh, &wg)
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

	for {
		select {
		case err := <-errCh:
			numErr++
			if !*tolerateErrors {
				log.Fatal(err)
			} else {
				if *verbose {
					log.Print(err)
				}
			}
			continue

		case <-tick:
			now := time.Now()
			elapsed := time.Since(lastNow)

			globalStatsMu.Lock()
			for i := 0; i < len(globalStats); i++ {
				lastGlobalStats[i] = globalStats[i]
			}
			updateGlobalStatsLocked(workers)
			lastOpsCount = opsCount
			opsCount = globalStats[writes] + globalStats[emptyReads] +
				globalStats[nonEmptyReads] + globalStats[scans]
			statsString := fmt.Sprintf(" (reads+:%d) (reads-:%d) (scans:%d) (writes:%d) (readErrors:%d) (scanErrors:%d) (writeErrors:%d)",
				globalStats[nonEmptyReads]-lastGlobalStats[nonEmptyReads],
				globalStats[emptyReads]-lastGlobalStats[emptyReads],
				globalStats[scans]-lastGlobalStats[scans],
				globalStats[writes]-lastGlobalStats[writes],
				globalStats[readErrors]-lastGlobalStats[readErrors],
				globalStats[scanErrors]-lastGlobalStats[scanErrors],
				globalStats[writeErrors]-lastGlobalStats[writeErrors],
			)
			globalStatsMu.Unlock()

			printStr := fmt.Sprintf("%s\t%6s: %6.1f/sec", *benchmarkName,
				time.Duration(time.Since(start).Seconds()+0.5)*time.Second,
				float64(opsCount-lastOpsCount)/elapsed.Seconds())
			if *duration > 0 {
				// If the duration is limited, show the average performance since starting.
				printStr = fmt.Sprintf("%s  %6.1f/sec", printStr, float64(opsCount)/time.Since(start).Seconds())
			}

			printStr = fmt.Sprintf("%s%s", printStr, statsString)
			fmt.Println(printStr)
			lastOpsCount = opsCount
			lastNow = now

		case <-done:
			if numErr > 0 {
				fmt.Printf(" (%d total errors)\n", numErr)
			}
			fmt.Printf("\n")
			return
		}
	}
}
