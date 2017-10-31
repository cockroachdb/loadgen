package main

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
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
	"testing"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/tylertreat/hdrhistogram-writer"
	"golang.org/x/time/rate"

	// Import postgres driver.
	"github.com/lib/pq"
)

const aChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 "

var batch = flag.Int("batch", 1, "Number of rows to insert in a single SQL statement")
var concurrency = flag.Int("concurrency", 2*runtime.NumCPU(), "Number of concurrent writers inserting blocks")
var duration = flag.Duration("duration", 0, "The duration to run. If 0, run forever.")
var nullPct = flag.Int("null-percent", 5, "Percent random nulls.")
var maxRate = flag.Float64("max-rate", 0, "Maximum frequency of operations (reads/writes). If 0, no limit.")
var maxOps = flag.Uint64("max-ops", 0, "Maximum number of blocks to read/write")
var tolerateErrors = flag.Bool("tolerate-errors", false, "Keep running on error")
var usePrepared = flag.Bool("prepared", false, "Use prepared statement")
var pgHost = flag.String("host", "localhost", "database host name")
var pgPort = flag.Int("port", 26257, "database port number")
var pgUser = flag.String("user", "root", "database user name (without password)")
var pgMethod = flag.String("method", "upsert", "choice of DML name (insert, upsert, ioc-update (insert on conflict update), ioc-nothing (insert on conflict no dothing)")
var pgPrimary = flag.String("primary", "", "ioc-update and ioc-nothing require primary key")

// Output in HdrHistogram Plotter format. See https://hdrhistogram.github.io/HdrHistogram/plotFiles.html
var histFile = flag.String("hist-file", "", "Write histogram data to file for HdrHistogram Plotter, or stdout if - is specified.")

// numOps keeps a global count of successful operations.
var numOps uint64

const (
	minLatency = 100 * time.Microsecond
	maxLatency = 10 * time.Second
)

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <db> <table>\n\n", os.Args[0])
	flag.PrintDefaults()
}

type col struct {
	name          string
	dataType      string
	dataPrecision int
	dataScale     int
	cdefault      sql.NullString
	isNullable    string
}

func clampLatency(d, min, max time.Duration) time.Duration {
	if d < min {
		return min
	}
	if d > max {
		return max
	}
	return d
}

type worker struct {
	db         *sql.DB
	cols       []col
	insertStmt *sql.Stmt
	batch      int
	latency    struct {
		sync.Mutex
		*hdrhistogram.WindowedHistogram
	}
}

func newWorker(db *sql.DB, cols []col, batch int, insertStmt *sql.Stmt) *worker {
	b := &worker{db: db, cols: cols, batch: batch, insertStmt: insertStmt}
	b.latency.WindowedHistogram = hdrhistogram.NewWindowed(1,
		minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	return b
}

// randString makes a random string of length [1, 10].
func randString(strLen int) string {
	b := make([]byte, rand.Intn(strLen)+1)
	for i := range b {
		b[i] = aChars[rand.Intn(len(aChars))]
	}
	return string(b)
}

// run is an infinite loop in which the worker continuously attempts to
// read / write blocks of random data into a table in cockroach DB.
func (b *worker) run(errCh chan<- error, wg *sync.WaitGroup, limiter *rate.Limiter) {
	defer wg.Done()

	params := make([]interface{}, len(b.cols)*b.batch)
	for {
		// Limit how quickly the load generator sends requests based on --max-rate.
		if limiter != nil {
			if err := limiter.Wait(context.Background()); err != nil {
				panic(err)
			}
		}

		k := 0 // index into params
		for j := 0; j < b.batch; j++ {
			for _, c := range b.cols {
				if c.isNullable == "YES" && rand.Intn(100) < *nullPct {
					switch c.dataType {
					case "BOOL":
						params[k] = sql.NullBool{Valid: false}
					case "FLOAT":
						params[k] = sql.NullFloat64{Valid: false}
					case "DECIMAL", "INT8", "BIGINT", "INT", "INTEGER", "INT4", "SMALLINT", "INT2":
						params[k] = sql.NullInt64{Valid: false}
					case "BYTES", "STRING":
						params[k] = sql.NullString{Valid: false}
					case "DATE", "TIMESTAMP":
						params[k] = pq.NullTime{Valid: false}
					default:
						log.Fatalf("Unsupported nullable type %s", c.dataType)
					}
				} else {
					switch c.dataType {
					case "BOOL":
						if rand.Intn(2) == 0 {
							params[k] = false
						} else {
							params[k] = true
						}
					case "DECIMAL", "FLOAT":
						params[k] = rand.Intn(100)
					case "BIGINT", "INT8":
						params[k] = rand.Int63()
					case "INT", "INTEGER", "INT4":
						// using Postgres specs on integer range so that test data could be used for Postgres and CockroachDB
						// https://www.postgresql.org/docs/current/static/datatype-numeric.html
						// https://www.cockroachlabs.com/docs/stable/int.html
						// Type         CRDB   Postgres
						// INT, Integer 64bits 32bits
						params[k] = rand.Int31()
					case "SMALLINT", "INT2":
						params[k] = int16(rand.Intn(32767))
					case "BYTES":
						b := make([]byte, 32)
						if _, err := rand.Read(b); err != nil {
							log.Fatal(err)
						}
						params[k] = fmt.Sprintf("%X", b)
					case "STRING":
						if c.dataPrecision == 0 {
							params[k] = randString(32)
						} else {
							params[k] = randString(c.dataPrecision)
						}
					case "DATE", "TIMESTAMP":
						params[k] = time.Unix(rand.Int63n(time.Now().Unix()-94608000)+94608000, 0)
					default:
						log.Fatalf("Unsupported type %s", c.dataType)
					}
				}
				k += 1
			}
		}

		start := time.Now()
		if _, err := b.insertStmt.Exec(params...); err != nil {
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

func getInsertStmt(db *sql.DB, dbName, tableName string, cols []col) (*sql.Stmt, error) {
	var buf bytes.Buffer
	var dmlMethod string
	var dmlSuffix bytes.Buffer

	switch *pgMethod {
	case "insert":
		dmlMethod = "insert"
		dmlSuffix.WriteString("")
	case "upsert":
		dmlMethod = "upsert"
		dmlSuffix.WriteString("")
	case "ioc-nothing":
		dmlMethod = "insert"
		dmlSuffix.WriteString(fmt.Sprintf(" on conflict (%s) do nothing", *pgPrimary))
	case "ioc-update":
		dmlMethod = "insert"
		dmlSuffix.WriteString(fmt.Sprintf(" on conflict (%s) do update set ", *pgPrimary))
		for i, c := range cols {
			if i > 0 {
				dmlSuffix.WriteString(",")
			}
			dmlSuffix.WriteString(fmt.Sprintf("%s=EXCLUDED.%s", c.name, c.name))
		}
	default:
		log.Fatal(fmt.Sprintf("%s DML method not valid", *pgMethod))
	}

	fmt.Fprintf(&buf, `%s INTO %s.%s (`, dmlMethod, dbName, tableName)
	for i, c := range cols {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(c.name)
	}
	buf.WriteString(`) VALUES `)

	nCols := len(cols)
	for i := 0; i < *batch; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("(")
		for j, _ := range cols {
			if j > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, `$%d`, 1+j+(nCols*i))
		}
		buf.WriteString(")")
	}

	buf.WriteString(dmlSuffix.String())

	if testing.Verbose() {
		fmt.Println(buf.String())
	}

	return db.Prepare(buf.String())
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(1)
	}

	dbName := flag.Arg(0)
	tableName := flag.Arg(1)

	dbURL := fmt.Sprintf("postgres://%s@%s:%d/%s?sslmode=disable", *pgUser, *pgHost, *pgPort, dbName)
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		log.Fatal(err)
	}

	// Open connection to server and create a database.
	db, dbErr := sql.Open("postgres", parsedURL.String())
	if dbErr != nil {
		log.Fatal(dbErr)
	}

	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(*concurrency + 1)
	db.SetMaxIdleConns(*concurrency + 1)

	rows, err := db.Query("SELECT column_name, data_type, column_default, is_nullable FROM information_schema.columns WHERE table_name = $1 and table_schema=$2", tableName, dbName)
	if err != nil {
		log.Fatal(err)
	}

	var cols []col
	var numCols = 0

	defer rows.Close()
	for rows.Next() {
		var col col
		col.dataPrecision = 0
		col.dataScale = 0

		if err := rows.Scan(&col.name, &col.dataType, &col.cdefault, &col.isNullable); err != nil {
			log.Fatal(err)
		}
		if col.cdefault.String == "unique_rowid()" { // skip
			continue
		}
		if strings.HasPrefix(col.cdefault.String, "uuid_v4()") { // skip
			continue
		}

		// ex: convert
		// DECIMAL(15,2) to DECIMAL 15 2
		// STRING(2) to STRING 20
		dataTypes := strings.FieldsFunc(col.dataType, func(r rune) bool { return r == '(' || r == ',' || r == ')' })
		if len(dataTypes) > 1 {
			col.dataType = dataTypes[0]
			if col.dataPrecision, err = strconv.Atoi(dataTypes[1]); err != nil {
				log.Fatal(err)
			}
		}
		if len(dataTypes) > 2 {
			if col.dataScale, err = strconv.Atoi(dataTypes[2]); err != nil {
				log.Fatal(err)
			}
		}

		cols = append(cols, col)
		numCols += 1
	}

	if numCols == 0 {
		log.Fatal("no columns detected")
	}

	// insert on conflict requires the primary key if any from information.schema
	if strings.HasPrefix(*pgMethod, "ioc") && *pgPrimary == "" {
		rows, err := db.Query("SELECT column_name FROM information_schema.key_column_usage WHERE constraint_name='primary' and table_name = $1 and table_schema=$2 order by ordinal_position", tableName, dbName)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var colname string

			if err := rows.Scan(&colname); err != nil {
				log.Fatal(err)
			}
			if *pgPrimary != "" {
				*pgPrimary += "," + colname
			} else {
				*pgPrimary += colname
			}
		}
	}

	if strings.HasPrefix(*pgMethod, "ioc") && *pgPrimary == "" {
		log.Fatal("inset on conflict requires primary key to be specified via -primary if the table does not have primary key")
		os.Exit(1)
	}

	insertStmt, err := getInsertStmt(db, dbName, tableName, cols)
	if err != nil {
		log.Fatal(err)
	}

	var limiter *rate.Limiter
	if *maxRate > 0 {
		// Create a limiter using maxRate specified on the command line and
		// with allowed burst of 1 at the maximum allowed rate.
		limiter = rate.NewLimiter(rate.Limit(*maxRate), 1)
	}

	lastNow := time.Now()
	start := lastNow
	var lastOps uint64
	workers := make([]*worker, *concurrency)

	errCh := make(chan error)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		workers[i] = newWorker(db, cols, *batch, insertStmt)
		go workers[i].run(errCh, &wg, limiter)
	}

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

	defer func() {
		// Output results that mimic Go's built-in benchmark format.

		benchmarkName := strings.Join([]string{
			"BenchmarkLoadgenRand",
			fmt.Sprintf("concurrency=%d", *concurrency),
			fmt.Sprintf("duration=%s", *duration),
		}, "/")

		result := testing.BenchmarkResult{
			N: int(numOps),
			T: time.Since(start),
		}
		fmt.Printf("%s\t%s\n", benchmarkName, result)
	}()

	cumLatency := hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)

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
			for _, w := range workers {
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

			cumLatency.Merge(h)
			p50 := h.ValueAtQuantile(50)
			p95 := h.ValueAtQuantile(95)
			p99 := h.ValueAtQuantile(99)
			pMax := h.ValueAtQuantile(100)

			now := time.Now()
			elapsed := now.Sub(lastNow)
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

			ops := atomic.LoadUint64(&numOps)
			elapsed := time.Since(start).Seconds()
			fmt.Println("\n_elapsed___errors_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			fmt.Printf("%7.1fs %8d %14d %14.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n\n",
				time.Since(start).Seconds(), numErr,
				ops, float64(ops)/elapsed,
				time.Duration(avg).Seconds()*1000,
				time.Duration(p50).Seconds()*1000,
				time.Duration(p95).Seconds()*1000,
				time.Duration(p99).Seconds()*1000,
				time.Duration(pMax).Seconds()*1000)
			if *histFile == "-" {
				if err := histwriter.WriteDistribution(cumLatency, nil, 1, os.Stdout); err != nil {
					fmt.Printf("failed to write histogram to stdout: %v\n", err)
				}
			} else if *histFile != "" {
				if err := histwriter.WriteDistributionFile(cumLatency, nil, 1, *histFile); err != nil {
					fmt.Printf("failed to write histogram file: %v\n", err)
				}
			}
			return
		}
	}
}
