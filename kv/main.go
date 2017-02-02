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
	"gopkg.in/mgo.v2/bson"

	"github.com/codahale/hdrhistogram"
	"github.com/gocql/gocql"
	// Import postgres driver.
	_ "github.com/lib/pq"
)

var readPercent = flag.Int("read-percent", 0, "Percent of operations that are reads")

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

var maxOps = flag.Uint64("max-ops", 0, "Maximum number of blocks to read/write")
var duration = flag.Duration("duration", 0, "The duration to run. If 0, run forever.")
var benchmarkName = flag.String("benchmark-name", "BenchmarkBlocks", "Test name to report "+
	"for Go benchmark results.")

// numOps keeps a global count of successful operations.
var numOps uint64

const (
	minLatency = 100 * time.Microsecond
	maxLatency = 10 * time.Second
)

func clampLatency(d, min, max time.Duration) time.Duration {
	if d < min {
		return min
	}
	if d > max {
		return max
	}
	return d
}

func randomBlock(r *rand.Rand) []byte {
	blockSize := r.Intn(*maxBlockSizeBytes-*minBlockSizeBytes) + *minBlockSizeBytes
	blockData := make([]byte, blockSize)
	for i := range blockData {
		blockData[i] = byte(r.Int() & 0xff)
	}
	return blockData
}

type database interface {
	read(blockID int64) error
	write(count int, r *rand.Rand) error
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
		minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	return b
}

// run is an infinite loop in which the blocker continuously attempts to
// write blocks of random data into a table in cockroach DB.
func (b *blocker) run(errCh chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		start := time.Now()
		var err error
		read := b.rand.Intn(100) < *readPercent
		if read {
			err = b.db.read(b.rand.Int63())
		} else {
			err = b.db.write(*batch, b.rand)
		}
		if err != nil {
			errCh <- err
			continue
		}
		elapsed := clampLatency(time.Since(start), minLatency, maxLatency)
		b.latency.Lock()
		if err := b.latency.Current.RecordValue(elapsed.Nanoseconds()); err != nil {
			log.Fatal(err)
		}
		b.latency.Unlock()
		v := atomic.AddUint64(&numOps, uint64(*batch))
		if *maxOps > 0 && v >= *maxOps {
			return
		}
	}
}

type cockroach struct {
	db        *sql.DB
	readStmt  *sql.Stmt
	writeStmt *sql.Stmt
}

func (c *cockroach) read(k int64) error {
	var v []byte
	if err := c.readStmt.QueryRow(k).Scan(&v); err != nil && err != sql.ErrNoRows {
		return err
	}
	return nil
}

func (c *cockroach) write(count int, r *rand.Rand) error {
	const argCount = 2
	args := make([]interface{}, argCount*count)
	for i := 0; i < count; i++ {
		j := i * argCount
		args[j+0] = r.Int63()
		args[j+1] = randomBlock(r)
	}
	// TODO(peter): The key generation is not guaranteed unique. Consider using
	// UPSERT, though initial tests show that is half the speed. Or perhaps
	// ignoring duplicate key violation errors.
	_, err := c.writeStmt.Exec(args...)
	return err
}

func setupCockroach(parsedURL *url.URL) (database, error) {
	// Open connection to server and create a database.
	db, dbErr := sql.Open("postgres", parsedURL.String())
	if dbErr != nil {
		return nil, dbErr
	}
	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		return nil, err
	}

	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(*concurrency + 1)
	db.SetMaxIdleConns(*concurrency + 1)

	if _, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS test.kv (
	  k BIGINT NOT NULL PRIMARY KEY,
	  v BYTES NOT NULL
	)`); err != nil {
		return nil, err
	}

	if *splits > 0 {
		r := rand.New(rand.NewSource(int64(time.Now().UnixNano())))
		for i := 0; i < *splits; i++ {
			if _, err := db.Exec(`ALTER TABLE test.kv SPLIT AT ($1)`, r.Int63()); err != nil {
				return nil, err
			}
		}
	}

	readStmt, err := db.Prepare(`SELECT v FROM test.kv WHERE k >= $1 LIMIT 1`)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	buf.WriteString(`INSERT INTO test.kv (k, v) VALUES`)

	for i := 0; i < *batch; i++ {
		j := i * 2
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, ` ($%d, $%d)`, j+1, j+2)
	}

	writeStmt, err := db.Prepare(buf.String())
	if err != nil {
		return nil, err
	}

	return &cockroach{db: db, readStmt: readStmt, writeStmt: writeStmt}, nil
}

type mongoBlock struct {
	Key   int64 `bson:"_id"`
	Value []byte
}

type mongo struct {
	kv *mgo.Collection
}

func (m *mongo) read(blockID int64) error {
	var b mongoBlock
	if err := m.kv.Find(bson.M{"_id": bson.M{"$gte": blockID}}).One(&b); err != nil {
		if err == mgo.ErrNotFound {
			return nil
		}
		return err
	}
	return nil
}

func (m *mongo) write(count int, r *rand.Rand) error {
	docs := make([]interface{}, count)
	for i := 0; i < count; i++ {
		docs[i] = &mongoBlock{
			Key:   r.Int63(),
			Value: randomBlock(r),
		}
	}

	return m.kv.Insert(docs...)
}

func setupMongo(parsedURL *url.URL) (database, error) {
	session, err := mgo.Dial(parsedURL.String())
	if err != nil {
		panic(err)
	}

	session.SetMode(mgo.Monotonic, true)
	// session.SetSafe(&mgo.Safe{FSync: true})

	kv := session.DB("test").C("kv")
	// if err := kv.DropCollection(); err != nil && err != mgo.ErrNotFound {
	// 	return nil, err
	// }
	return &mongo{kv: kv}, nil
}

type cassandra struct {
	session *gocql.Session
}

func (c *cassandra) read(blockID int64) error {
	// TODO(peter): This doesn't work because:
	//
	//     Only EQ and IN relation are supported on the partition key
	//     (unless you use the token() function).
	var v []byte
	if err := c.session.Query(
		`SELECT v FROM test.kv WHERE k >= ? LIMIT 1`,
		blockID).Consistency(gocql.One).Scan(&v); err != nil {
		if err == gocql.ErrNotFound {
			return nil
		}
		return err
	}
	return nil
}

func (c *cassandra) write(count int, r *rand.Rand) error {
	const insertBlockStmt = "INSERT INTO test.kv (k, v) VALUES (?, ?); "

	var buf bytes.Buffer
	buf.WriteString("BEGIN BATCH ")
	const argCount = 2
	args := make([]interface{}, argCount*count)

	for i := 0; i < count; i++ {
		j := i * argCount
		args[j+0] = r.Int63()
		args[j+1] = randomBlock(r)
		buf.WriteString(insertBlockStmt)
	}

	buf.WriteString("APPLY BATCH;")
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
CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 1
};`
	const createTable = `
CREATE TABLE IF NOT EXISTS test.kv(
  k BIGINT,
  v BLOB,
  PRIMARY KEY(k)
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
	parsedURL.Path = "test"

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
	var lastOps uint64
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
			*benchmarkName, numOps, float64(elapsed.Nanoseconds())/float64(numOps))
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
			ops := atomic.LoadUint64(&numOps)
			if i%20 == 0 {
				fmt.Println("_elapsed___errors__ops/sec(inst)___ops/sec(cum)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			i++
			fmt.Printf("%8s %8d %14.1f %14.1f %8.1f %8.1f %8.1f %8.1f\n",
				time.Duration(time.Since(start).Seconds()+0.5)*time.Second,
				numErr,
				float64(ops-lastOps)/elapsed.Seconds(),
				float64(ops)/time.Since(start).Seconds(),
				time.Duration(p50).Seconds()*1000,
				time.Duration(p95).Seconds()*1000,
				time.Duration(p99).Seconds()*1000,
				time.Duration(pMax).Seconds()*1000)
			lastOps = ops
			lastNow = now

		case <-done:
			ops := atomic.LoadUint64(&numOps)
			elapsed := time.Since(start).Seconds()
			fmt.Println("\n_elapsed___errors____________ops___ops/sec(cum)")
			fmt.Printf("%7.1fs %8d %14d %14.1f\n\n",
				time.Since(start).Seconds(), numErr,
				ops, float64(ops)/elapsed)
			return
		}
	}
}
