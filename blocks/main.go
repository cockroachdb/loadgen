// Copyright 2015 The Cockroach Authors.
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
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	mgo "gopkg.in/mgo.v2"

	"github.com/codahale/hdrhistogram"
	"github.com/gocql/gocql"
	"github.com/satori/go.uuid"
	// Import postgres driver.
	_ "github.com/lib/pq"
)

const (
	insertBlockStmt = `INSERT INTO blocks (block_id, writer_id, block_num, raw_bytes) VALUES`
)

// concurrency = number of concurrent insertion processes.
var concurrency = flag.Int("concurrency", 2*runtime.NumCPU(), "Number of concurrent writers inserting blocks")

// batch = number of blocks to insert in a single SQL statement.
var batch = flag.Int("batch", 1, "Number of blocks to insert in a single SQL statement")

var splits = flag.Int("splits", 0, "Number of splits to perform before starting normal operations")

var tolerateErrors = flag.Bool("tolerate-errors", false, "Keep running on error")

// outputInterval = interval at which information is output to console.
var outputInterval = flag.Duration("output-interval", 1*time.Second, "Interval of output")

// Minimum and maximum size of inserted blocks.
var minBlockSizeBytes = flag.Int("min-block-bytes", 256, "Minimum amount of raw data written with each insertion")
var maxBlockSizeBytes = flag.Int("max-block-bytes", 1024, "Maximum amount of raw data written with each insertion")

var maxBlocks = flag.Uint64("max-blocks", 0, "Maximum number of blocks to write")
var duration = flag.Duration("duration", 0, "The duration to run. If 0, run forever.")
var benchmarkName = flag.String("benchmark-name", "BenchmarkBlocks", "Test name to report "+
	"for Go benchmark results.")

// numBlocks keeps a global count of successfully written blocks.
var numBlocks uint64

func randomBlock(r *rand.Rand) []byte {
	blockSize := r.Intn(*maxBlockSizeBytes-*minBlockSizeBytes) + *minBlockSizeBytes
	blockData := make([]byte, blockSize)
	for i := range blockData {
		blockData[i] = byte(r.Int() & 0xff)
	}
	return blockData
}

type database interface {
	write(writerID string, blockNum int64, blockCount int, r *rand.Rand) error
}

type blocker struct {
	db      database
	rand    *rand.Rand
	latency struct {
		sync.Mutex
		*hdrhistogram.WindowedHistogram
	}
}

func newBlocker(db database) *blocker {
	b := &blocker{
		db:   db,
		rand: rand.New(rand.NewSource(int64(time.Now().UnixNano()))),
	}
	b.latency.WindowedHistogram = hdrhistogram.NewWindowed(1,
		(100 * time.Microsecond).Nanoseconds(),
		(10 * time.Second).Nanoseconds(),
		1)
	return b
}

// run is an infinite loop in which the blocker continuously attempts to
// write blocks of random data into a table in cockroach DB.
func (b *blocker) run(errCh chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	writerID := uuid.NewV4().String()
	for blockNum := int64(0); ; blockNum += int64(*batch) {
		start := time.Now()
		if err := b.db.write(writerID, blockNum, *batch, b.rand); err != nil {
			errCh <- err
		} else {
			elapsed := time.Since(start)
			b.latency.Lock()
			if err := b.latency.Current.RecordValue(elapsed.Nanoseconds()); err != nil {
				log.Fatal(err)
			}
			b.latency.Unlock()
			v := atomic.AddUint64(&numBlocks, uint64(*batch))
			if *maxBlocks > 0 && v >= *maxBlocks {
				return
			}
		}
	}
}

type cockroach struct {
	db   *sql.DB
	stmt *sql.Stmt
}

func (c *cockroach) write(writerID string, blockNum int64, blockCount int, r *rand.Rand) error {
	const argCount = 4
	args := make([]interface{}, argCount*blockCount)
	for i := 0; i < blockCount; i++ {
		j := i * argCount
		args[j+0] = r.Int63()
		args[j+1] = writerID
		args[j+2] = blockNum + int64(i)
		args[j+3] = randomBlock(r)
	}
	_, err := c.stmt.Exec(args...)
	return err
}

func setupCockroach(parsedURL *url.URL) (database, error) {
	// Open connection to server and create a database.
	db, dbErr := sql.Open("postgres", parsedURL.String())
	if dbErr != nil {
		return nil, dbErr
	}
	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS datablocks"); err != nil {
		return nil, err
	}

	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(*concurrency + 1)
	db.SetMaxIdleConns(*concurrency + 1)

	// Create the initial table for storing blocks.
	if _, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS blocks (
	  block_id BIGINT NOT NULL,
	  writer_id STRING NOT NULL,
	  block_num BIGINT NOT NULL,
	  raw_bytes BYTES NOT NULL,
	  PRIMARY KEY (block_id, writer_id, block_num)
	)`); err != nil {
		return nil, err
	}

	if *splits > 0 {
		r := rand.New(rand.NewSource(int64(time.Now().UnixNano())))
		for i := 0; i < *splits; i++ {
			if _, err := db.Exec(`ALTER TABLE blocks SPLIT AT ($1, '', 0)`, r.Int63()); err != nil {
				return nil, err
			}
		}
	}

	var buf bytes.Buffer
	if _, err := buf.WriteString(`INSERT INTO blocks (block_id, writer_id, block_num, raw_bytes) VALUES`); err != nil {
		return nil, err
	}

	for i := 0; i < *batch; i++ {
		j := i * 4
		if i > 0 {
			if _, err := buf.WriteString(", "); err != nil {
				return nil, err
			}
		}
		fmt.Fprintf(&buf, ` ($%d, $%d, $%d, $%d)`, j+1, j+2, j+3, j+4)
	}

	stmt, err := db.Prepare(buf.String())
	if err != nil {
		return nil, err
	}

	return &cockroach{db: db, stmt: stmt}, nil
}

type mongo struct {
	blocks *mgo.Collection
}

func (m *mongo) write(writerID string, blockNum int64, blockCount int, r *rand.Rand) error {
	type Block struct {
		BlockID  int64
		WriterID string
		BlockNum int64
		RawBytes []byte
	}
	docs := make([]interface{}, blockCount)
	for i := 0; i < blockCount; i++ {
		docs[i] = &Block{
			BlockID:  r.Int63(),
			WriterID: writerID,
			BlockNum: blockNum + int64(i),
			RawBytes: randomBlock(r),
		}
	}

	return m.blocks.Insert(docs...)
}

func setupMongo(parsedURL *url.URL) (database, error) {
	session, err := mgo.Dial(parsedURL.String())
	if err != nil {
		panic(err)
	}

	session.SetMode(mgo.Monotonic, true)
	// session.SetSafe(&mgo.Safe{FSync: true})

	blocks := session.DB("datablocks").C("blocks")
	return &mongo{blocks: blocks}, nil
}

type cassandra struct {
	session *gocql.Session
}

func (c *cassandra) write(writerID string, blockNum int64, blockCount int, r *rand.Rand) error {
	const insertBlockStmt = "INSERT INTO datablocks.blocks " +
		"(block_id, writer_id, block_num, raw_bytes) VALUES (?, ?, ?, ?); "

	var buf bytes.Buffer
	if _, err := buf.WriteString("BEGIN BATCH "); err != nil {
		return err
	}
	args := make([]interface{}, 4*blockCount)

	for i := 0; i < blockCount; i++ {
		j := i * 4
		args[j+0] = r.Int63()
		args[j+1] = writerID
		args[j+2] = blockNum + int64(i)
		args[j+3] = randomBlock(r)
		if _, err := buf.WriteString(insertBlockStmt); err != nil {
			return err
		}
	}

	if _, err := buf.WriteString("APPLY BATCH;"); err != nil {
		return err
	}
	return c.session.Query(buf.String(), args...).Exec()
}

func setupCassandra(parsedURL *url.URL) (database, error) {
	cluster := gocql.NewCluster(parsedURL.Host)
	cluster.Consistency = gocql.Quorum
	s, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	const createKeyspace = `
CREATE KEYSPACE IF NOT EXISTS datablocks WITH REPLICATION = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 1
};`
	const createTable = `
CREATE TABLE IF NOT EXISTS datablocks.blocks(
  block_id BIGINT,
  writer_id TEXT,
  block_num BIGINT,
  raw_bytes BLOB,
  PRIMARY KEY(block_id, writer_id, block_num)
);`

	if err := s.Query(createKeyspace).RetryPolicy(nil).Exec(); err != nil {
		log.Fatal(err)
	}
	if err := s.Query(createTable).RetryPolicy(nil).Exec(); err != nil {
		log.Fatal(err)
	}
	return &cassandra{session: s}, nil
}

// setupDatabase performs initial setup for the example, creating a database and
// with a single table. If the desired table already exists on the cluster, the
// existing table will be dropped.
func setupDatabase(dbURL string) (database, error) {
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}
	parsedURL.Path = "datablocks"

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

func main() {
	flag.Usage = usage
	flag.Parse()

	dbURL := "postgres://root@localhost:26257/photos?sslmode=disable"
	if flag.NArg() == 1 {
		dbURL = flag.Arg(0)
	}

	if *concurrency < 1 {
		log.Fatalf("Value of 'concurrency' flag (%d) must be greater than or equal to 1", *concurrency)
	}

	if max, min := *maxBlockSizeBytes, *minBlockSizeBytes; max < min {
		log.Fatalf("Value of 'max-block-bytes' (%d) must be greater than or equal to value of 'min-block-bytes' (%d)", max, min)
	}

	var db database
	{
		var err error
		for err == nil || *tolerateErrors {
			db, err = setupDatabase(dbURL)
			if err == nil {
				break
			}
			if !*tolerateErrors {
				log.Fatal(err)
			}
		}
	}

	lastNow := time.Now()
	start := lastNow
	var lastBlocks uint64
	writers := make([]*blocker, *concurrency)

	errCh := make(chan error)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		writers[i] = newBlocker(db)
		go writers[i].run(errCh, &wg)
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
			*benchmarkName, numBlocks, float64(elapsed.Nanoseconds())/float64(numBlocks))
	}()

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
			for _, w := range writers {
				w.latency.Lock()
				m := w.latency.Merge()
				w.latency.Rotate()
				w.latency.Unlock()
				if h == nil {
					h = m
				} else {
					h.Merge(m)
				}
			}

			p50 := h.ValueAtQuantile(50)
			p95 := h.ValueAtQuantile(95)
			p99 := h.ValueAtQuantile(99)
			pMax := h.ValueAtQuantile(100)

			now := time.Now()
			elapsed := time.Since(lastNow)
			blocks := atomic.LoadUint64(&numBlocks)
			if i%20 == 0 {
				fmt.Println("_elapsed___errors__ops/sec(inst)___ops/sec(cum)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			i++
			fmt.Printf("%8s %8d %14.1f %14.1f %8.1f %8.1f %8.1f %8.1f\n",
				time.Duration(time.Since(start).Seconds()+0.5)*time.Second,
				numErr,
				float64(blocks-lastBlocks)/elapsed.Seconds(),
				float64(blocks)/time.Since(start).Seconds(),
				time.Duration(p50).Seconds()*1000,
				time.Duration(p95).Seconds()*1000,
				time.Duration(p99).Seconds()*1000,
				time.Duration(pMax).Seconds()*1000)
			lastBlocks = blocks
			lastNow = now

		case <-done:
			blocks := atomic.LoadUint64(&numBlocks)
			elapsed := time.Since(start).Seconds()
			fmt.Println("\n_elapsed___errors_________blocks___ops/sec(cum)")
			fmt.Printf("%7.1fs %8d %14d %14.1f\n\n",
				time.Since(start).Seconds(), numErr,
				blocks, float64(blocks)/elapsed)
			return
		}
	}
}
