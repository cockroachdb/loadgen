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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/gocql/gocql"
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

// Mongo flags. See https://godoc.org/gopkg.in/mgo.v2#Session.SetSafe for details.
var mongoWMode = flag.String("mongo-wmode", "", "WMode for mongo session (eg: majority)")
var mongoJ = flag.Bool("mongo-j", false, "Sync journal before op return")

// Cassandra flags.
var cassandraConsistency = flag.String("cassandra-consistency", "QUORUM", "Op consistency: ANY ONE TWO THREE QUORUM ALL LOCAL_QUORUM EACH_QUORUM LOCAL_ONE")
var cassandraReplication = flag.Int("cassandra-replication", 1, "Replication factor for cassandra")

type database interface {
	readRow(key uint64) (bool, error)
	insertRow(key uint64, fields []string) error
	clone() database
}

// ycsbWorker independently issues reads, writes, and scans against the database.
type ycsbWorker struct {
	db database
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

func newYcsbWorker(db database, zipfR *ZipfGenerator, workloadFlag string) *ycsbWorker {
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
	defer wg.Done()
	for i := uint64(thisWorkerNum + zipfIMin); i < n; i += uint64(numWorkers) {
		hashedKey := yw.hashKey(i, *maxWrites)
		if err := yw.insertRow(hashedKey, false); err != nil {
			if *verbose {
				fmt.Printf("error loading row %d: %s\n", i, err)
			}
			atomic.AddUint64(&globalStats[writeErrors], 1)
		} else if *verbose {
			fmt.Printf("loaded %d -> %d\n", i, hashedKey)
		}
	}
}

// runWorker is an infinite loop in which the ycsbWorker reads and writes
// random data into the table in proportion to the op frequencies.
func (yw *ycsbWorker) runWorker(errCh chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

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
	fields := make([]string, numTableFields)
	for i := 0; i < len(fields); i++ {
		fields[i] = yw.randString(fieldLength)
	}
	if err := yw.db.insertRow(key, fields); err != nil {
		return err
	}

	if increment {
		if err := yw.zipfR.IncrementIMax(); err != nil {
			return err
		}
	}
	atomic.AddUint64(&globalStats[writes], 1)
	return nil
}

func (yw *ycsbWorker) readRow() error {
	empty, err := yw.db.readRow(yw.nextReadKey())
	if err != nil {
		return err
	}
	if !empty {
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

type cockroach struct {
	db *sql.DB
}

func (c *cockroach) readRow(key uint64) (bool, error) {
	res, err := c.db.Query(fmt.Sprintf("SELECT * FROM ycsb.usertable WHERE ycsb_key=%d", key))
	if err != nil {
		return false, err
	}
	var rowsFound int
	for res.Next() {
		rowsFound++
	}
	if *verbose {
		fmt.Printf("reader found %d rows for key %d\n", rowsFound, key)
	}
	if err := res.Close(); err != nil {
		return false, err
	}
	return rowsFound == 0, nil
}

func (c *cockroach) insertRow(key uint64, fields []string) error {
	// TODO(arjun): Consider using a prepared statement here.
	stmt := fmt.Sprintf("INSERT INTO ycsb.usertable VALUES (%d, %s)",
		key, strings.Join(fields, ", "))
	_, err := c.db.Exec(stmt)
	return err
}

func (c *cockroach) clone() database {
	return c
}

func setupCockroach(parsedURL *url.URL) (database, error) {
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

	if *splits > 0 {
		// NB: We only need ycsbWorker.hashKey, so passing nil for the database and
		// ZipfGenerator is ok.
		w := newYcsbWorker(nil, nil, *workload)
		for i := 0; i < *splits; i++ {
			key := w.hashKey(uint64(i), *maxWrites)
			if _, err := db.Exec(`ALTER TABLE ycsb.usertable SPLIT AT VALUES ($1)`, key); err != nil {
				log.Fatal(err)
			}
		}
	}

	return &cockroach{db: db}, nil
}

type mongoBlock struct {
	Key    int64 `bson:"_id"`
	Fields []string
}

type mongo struct {
	kv *mgo.Collection
}

func (m *mongo) readRow(key uint64) (bool, error) {
	var b mongoBlock
	if err := m.kv.Find(bson.M{"_id": key}).One(&b); err != nil {
		if err == mgo.ErrNotFound {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func (m *mongo) insertRow(key uint64, fields []string) error {
	return m.kv.Insert(&mongoBlock{
		Key:    int64(key),
		Fields: fields,
	})
}

func (m *mongo) clone() database {
	return &mongo{
		// NB: Whoa!
		kv: m.kv.Database.Session.Copy().DB(m.kv.Database.Name).C(m.kv.Name),
	}
}

func setupMongo(parsedURL *url.URL) (database, error) {
	session, err := mgo.Dial(parsedURL.String())
	if err != nil {
		panic(err)
	}

	session.SetMode(mgo.Monotonic, true)
	session.SetSafe(&mgo.Safe{WMode: *mongoWMode, J: *mongoJ})

	kv := session.DB("ycsb").C("kv")
	if *drop {
		// Intentionally ignore the error as we can't tell if the collection
		// doesn't exist.
		_ = kv.DropCollection()
	}
	return &mongo{kv: kv}, nil
}

type cassandra struct {
	session *gocql.Session
}

func (c *cassandra) readRow(key uint64) (bool, error) {
	var k uint64
	var fields [10]string
	if err := c.session.Query(
		`SELECT * FROM ycsb.usertable WHERE ycsb_key = ? LIMIT 1`,
		key).Consistency(gocql.One).Scan(&k, &fields[0], &fields[1], &fields[2], &fields[3],
		&fields[4], &fields[5], &fields[6], &fields[7], &fields[8], &fields[9]); err != nil {
		if err == gocql.ErrNotFound {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func (c *cassandra) insertRow(key uint64, fields []string) error {
	const stmt = "INSERT INTO ycsb.usertable " +
		"(ycsb_key, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); "
	args := make([]interface{}, len(fields)+1)
	args[0] = key
	for i := 0; i < len(fields); i++ {
		args[i+1] = fields[i]
	}
	return c.session.Query(stmt, args...).Exec()
}

func (c *cassandra) clone() database {
	return c
}

func setupCassandra(parsedURL *url.URL) (database, error) {
	cluster := gocql.NewCluster(parsedURL.Host)
	cluster.Consistency = gocql.ParseConsistency(*cassandraConsistency)
	s, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	if *drop {
		_ = s.Query(`DROP KEYSPACE ycsb`).RetryPolicy(nil).Exec()
	}

	createKeyspace := fmt.Sprintf(`
CREATE KEYSPACE IF NOT EXISTS ycsb WITH REPLICATION = {
  'class' : 'SimpleStrategy',
  'replication_factor' : %d
};`, *cassandraReplication)

	const createTable = `
CREATE TABLE IF NOT EXISTS ycsb.usertable (
  ycsb_key BIGINT,
	FIELD1 BLOB,
	FIELD2 BLOB,
	FIELD3 BLOB,
	FIELD4 BLOB,
	FIELD5 BLOB,
	FIELD6 BLOB,
	FIELD7 BLOB,
	FIELD8 BLOB,
	FIELD9 BLOB,
	FIELD10 BLOB,
  PRIMARY KEY(ycsb_key)
);`

	if err := s.Query(createKeyspace).RetryPolicy(nil).Exec(); err != nil {
		log.Fatal(err)
	}
	if err := s.Query(createTable).RetryPolicy(nil).Exec(); err != nil {
		log.Fatal(err)
	}
	return &cassandra{session: s}, nil
}

// setupDatabase performs initial setup for the example, creating a database
// with a single table. If the desired table already exists on the cluster, the
// existing table will be dropped if the -drop flag was specified.
func setupDatabase(dbURL string) (database, error) {
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}
	parsedURL.Path = "ycsb"

	switch parsedURL.Scheme {
	case "postgres", "postgresql":
		return setupCockroach(parsedURL)
	case "mongodb":
		return setupMongo(parsedURL)
	case "cassandra":
		return setupCassandra(parsedURL)
	default:
		return nil, fmt.Errorf("unsupported database: %s", parsedURL.Scheme)
	}
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
		workers[i] = newYcsbWorker(db.clone(), zipfR, *workload)
	}

	loadStart := time.Now()
	var wg sync.WaitGroup
	// TODO(peter): Using all of the workers for loading leads to errors with
	// some of the insert statements receiving an EOF. For now, use a single
	// worker.
	for i, n := 0, 1; i < n; i++ {
		wg.Add(1)
		go workers[i].runLoader(*initialLoad, n, i, &wg)
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
