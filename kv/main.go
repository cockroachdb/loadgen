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
	"crypto/sha1"
	"database/sql"
	"encoding/binary"
	"flag"
	"fmt"
	"hash"
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
var minBlockSizeBytes = flag.Int("min-block-bytes", 1, "Minimum amount of raw data written with each insertion")
var maxBlockSizeBytes = flag.Int("max-block-bytes", 2, "Maximum amount of raw data written with each insertion")

var maxOps = flag.Uint64("max-ops", 0, "Maximum number of blocks to read/write")
var duration = flag.Duration("duration", 0, "The duration to run. If 0, run forever.")
var writeSeq = flag.Int64("write-seq", 0, "Initial write sequence value.")
var seqSeed = flag.Int64("seed", time.Now().UnixNano(), "Key hash seed.")
var drop = flag.Bool("drop", false, "Clear the existing data before starting.")
var benchmarkName = flag.String("benchmark-name", "BenchmarkBlocks", "Test name to report "+
	"for Go benchmark results.")

// Mongo flags. See https://godoc.org/gopkg.in/mgo.v2#Session.SetSafe for details.
var mongoWMode = flag.String("mongo-wmode", "", "WMode for mongo session (eg: majority)")
var mongoJ = flag.Bool("mongo-j", false, "Sync journal before op return")

// Cassandra flags.
var cassandraConsistency = flag.String("cassandra-consistency", "QUORUM", "Op consistency: ANY ONE TWO THREE QUORUM ALL LOCAL_QUORUM EACH_QUORUM LOCAL_ONE")
var cassandraReplication = flag.Int("cassandra-replication", 1, "Replication factor for cassandra")

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

type sequence struct {
	val  int64
	seed int64
}

func (s *sequence) write() int64 {
	return atomic.AddInt64(&s.val, 1) - 1
}

// read returns the max key index that has been written. Note that the returned
// index might not actually have been written yet, so a read operation cannot
// require that the key is present.
func (s *sequence) read() int64 {
	return atomic.LoadInt64(&s.val)
}

// generator generates read and write keys. Read keys are guaranteed to
// exist. A generator is NOT goroutine safe.
type generator struct {
	seq    *sequence
	rand   *rand.Rand
	hasher hash.Hash
	buf    [sha1.Size]byte
}

func newGenerator(seq *sequence) *generator {
	return &generator{
		seq:    seq,
		rand:   rand.New(rand.NewSource(int64(time.Now().UnixNano()))),
		hasher: sha1.New(),
	}
}

func (g *generator) hash(v int64) int64 {
	binary.BigEndian.PutUint64(g.buf[:8], uint64(v))
	binary.BigEndian.PutUint64(g.buf[8:16], uint64(g.seq.seed))
	g.hasher.Reset()
	_, _ = g.hasher.Write(g.buf[:16])
	g.hasher.Sum(g.buf[:0])
	return int64(binary.BigEndian.Uint64(g.buf[:8]))
}

func (g *generator) writeKey() int64 {
	return g.hash(g.seq.write())
}

func (g *generator) readKey() int64 {
	v := g.seq.read()
	if v == 0 {
		return 0
	}
	return g.hash(g.rand.Int63n(v))
}

func (g *generator) randomBlock() []byte {
	blockSize := g.rand.Intn(*maxBlockSizeBytes-*minBlockSizeBytes) + *minBlockSizeBytes
	blockData := make([]byte, blockSize)
	for i := range blockData {
		blockData[i] = byte(g.rand.Int() & 0xff)
	}
	return blockData
}

type database interface {
	read(key int64) error
	write(count int, g *generator) error
}

type blocker struct {
	db      database
	gen     *generator
	latency struct {
		sync.Mutex
		*hdrhistogram.WindowedHistogram
	}
}

func newBlocker(db database, seq *sequence) *blocker {
	b := &blocker{
		db:  db,
		gen: newGenerator(seq),
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
		if b.gen.rand.Intn(100) < *readPercent {
			err = b.db.read(b.gen.readKey())
		} else {
			err = b.db.write(*batch, b.gen)
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
	if err := c.readStmt.QueryRow(k).Scan(&k, &v); err != nil && err != sql.ErrNoRows {
		return err
	}
	return nil
}

func (c *cockroach) write(count int, g *generator) error {
	const argCount = 2
	args := make([]interface{}, argCount*count)
	for i := 0; i < count; i++ {
		j := i * argCount
		args[j+0] = g.writeKey()
		args[j+1] = g.randomBlock()
	}
	// TODO(peter): The key generation is not guaranteed unique. Consider using
	// UPSERT, though initial tests show that is half the speed. Or perhaps
	// ignoring duplicate key violation errors.
	_, err := c.writeStmt.Exec(args...)
	return err
}

func setupCockroach(parsedURL *url.URL, firstTime bool) (database, error) {
	// Open connection to server and create a database.
	db, dbErr := sql.Open("postgres", parsedURL.String())
	if dbErr != nil {
		return nil, dbErr
	}

	if firstTime {
		if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS test"); err != nil {
			return nil, err
		}

		if *drop {
			if _, err := db.Exec(`DROP TABLE IF EXISTS test.kv`); err != nil {
				return nil, err
			}
		}

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
	}

	readStmt, err := db.Prepare(`SELECT k, v FROM test.kv WHERE k = $1 LIMIT 1`)
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

func (m *mongo) read(key int64) error {
	var b mongoBlock
	// These queries are functionally the same, but the one using the $eq
	// operator is ~30% slower!
	//
	// if err := m.kv.Find(bson.M{"_id": bson.M{"$eq": key}}).One(&b); err != nil {
	if err := m.kv.Find(bson.M{"_id": key}).One(&b); err != nil {
		if err == mgo.ErrNotFound {
			return nil
		}
		return err
	}
	return nil
}

func (m *mongo) write(count int, g *generator) error {
	docs := make([]interface{}, count)
	for i := 0; i < count; i++ {
		docs[i] = &mongoBlock{
			Key:   g.writeKey(),
			Value: g.randomBlock(),
		}
	}

	return m.kv.Insert(docs...)
}

func setupMongo(parsedURL *url.URL, firstTime bool) (database, error) {
	session, err := mgo.Dial(parsedURL.String())
	if err != nil {
		panic(err)
	}

	session.SetMode(mgo.Monotonic, true)
	session.SetSafe(&mgo.Safe{WMode: *mongoWMode, J: *mongoJ})

	kv := session.DB("test").C("kv")
	if firstTime && *drop {
		// Intentionally ignore the error as we can't tell if the collection
		// doesn't exist.
		_ = kv.DropCollection()
	}
	return &mongo{kv: kv}, nil
}

type cassandra struct {
	session *gocql.Session
}

func (c *cassandra) read(key int64) error {
	var v []byte
	if err := c.session.Query(
		`SELECT v FROM test.kv WHERE k = ? LIMIT 1`,
		key).Consistency(gocql.One).Scan(&v); err != nil {
		if err == gocql.ErrNotFound {
			return nil
		}
		return err
	}
	return nil
}

func (c *cassandra) write(count int, g *generator) error {
	const insertBlockStmt = "INSERT INTO test.kv (k, v) VALUES (?, ?); "

	var buf bytes.Buffer
	buf.WriteString("BEGIN BATCH ")
	const argCount = 2
	args := make([]interface{}, argCount*count)

	for i := 0; i < count; i++ {
		j := i * argCount
		args[j+0] = g.writeKey()
		args[j+1] = g.randomBlock()
		buf.WriteString(insertBlockStmt)
	}

	buf.WriteString("APPLY BATCH;")
	return c.session.Query(buf.String(), args...).Exec()
}

func setupCassandra(parsedURL *url.URL, firstTime bool) (database, error) {
	cluster := gocql.NewCluster(parsedURL.Host)
	cluster.Consistency = gocql.ParseConsistency(*cassandraConsistency)
	s, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	if firstTime {
		if *drop {
			_ = s.Query(`DROP KEYSPACE test`).RetryPolicy(nil).Exec()
		}

		createKeyspace := fmt.Sprintf(`
CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {
  'class' : 'SimpleStrategy',
  'replication_factor' : %d
};`, *cassandraReplication)

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
	}
	return &cassandra{session: s}, nil
}

// setupDatabase performs initial setup for the example, creating a database and
// with a single table. If the desired table already exists on the cluster, the
// existing table will be dropped.
// If 'firstTime' is false, structures are not created or destroyed.
func setupDatabase(dbURL string, firstTime bool) (database, error) {
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}
	parsedURL.Path = "test"

	switch parsedURL.Scheme {
	case "postgres", "postgresql":
		return setupCockroach(parsedURL, firstTime)
	case "mongodb":
		return setupMongo(parsedURL, firstTime)
	case "cassandra":
		return setupCassandra(parsedURL, firstTime)
	default:
		return nil, fmt.Errorf("unsupported database: %s", parsedURL.Scheme)
	}
}

func trySetupDatabase(dbURL string, firstTime bool) database {
	var db database
	{
		var err error
		for err == nil || *tolerateErrors {
			db, err = setupDatabase(dbURL, firstTime)
			if err == nil {
				break
			}
			if !*tolerateErrors {
				log.Fatal(err)
			}
		}
	}
	return db
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

	lastNow := time.Now()
	start := lastNow
	var lastOps uint64
	writers := make([]*blocker, *concurrency)

	seq := &sequence{val: *writeSeq, seed: *seqSeed}
	errCh := make(chan error)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		db := trySetupDatabase(dbURL, i == 0 /* firstTime */)
		writers[i] = newBlocker(db, seq)
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
			fmt.Println("\n_elapsed___errors____________ops___ops/sec(cum)_____seq(begin/end)")
			fmt.Printf("%7.1fs %8d %14d %14.1f %18s\n\n",
				time.Since(start).Seconds(), numErr,
				ops, float64(ops)/elapsed,
				fmt.Sprintf("%d / %d", *writeSeq, seq.read()))
			return
		}
	}
}
