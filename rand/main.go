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
	_ "github.com/lib/pq"
)

var batch = flag.Int("batch", 1, "Number of rows to insert in a single SQL statement")
var concurrency = flag.Int("concurrency", 2*runtime.NumCPU(), "Number of concurrent writers inserting blocks")
var duration = flag.Duration("duration", 0, "The duration to run. If 0, run forever.")
var nullPct = flag.Int("null-percent", 0, "Percent random nulls.")
var maxRate = flag.Float64("max-rate", 0, "Maximum frequency of operations (reads/writes). If 0, no limit.")
var maxOps = flag.Uint64("max-ops", 0, "Maximum number of blocks to read/write")
var tolerateErrors = flag.Bool("tolerate-errors", false, "Keep running on error")

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
	name       string
	dataType   string
	cdefault   sql.NullString
	isNullable string
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
	latency    struct {
		sync.Mutex
		*hdrhistogram.WindowedHistogram
	}
}

func newWorker(db *sql.DB, cols []col, insertStmt *sql.Stmt) *worker {
	b := &worker{db: db, cols: cols, insertStmt: insertStmt}
	b.latency.WindowedHistogram = hdrhistogram.NewWindowed(1,
		minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	return b
}

// run is an infinite loop in which the worker continuously attempts to
// read / write blocks of random data into a table in cockroach DB.
func (b *worker) run(errCh chan<- error, wg *sync.WaitGroup, limiter *rate.Limiter) {
	defer wg.Done()

	params := make([]interface{}, len(b.cols))
	for {
		// Limit how quickly the load generator sends requests based on --max-rate.
		if limiter != nil {
			if err := limiter.Wait(context.Background()); err != nil {
				panic(err)
			}
		}

		for i, c := range b.cols {
			if c.isNullable == "YES" && rand.Intn(100) < *nullPct {
				switch c.dataType {
				case "INT":
					params[i] = sql.NullInt64{Valid: false}
				case "BYTES", "STRING":
					params[i] = sql.NullString{Valid: false}
				default:
					log.Fatalf("Unsupported nullable type %s", c.dataType)
				}
				continue
			}

			switch c.dataType {
			case "INT":
				params[i] = rand.Int63()
			case "BYTES", "STRING":
				b := make([]byte, 32)
				if _, err := rand.Read(b); err != nil {
					log.Fatal(err)
				}
				params[i] = fmt.Sprintf("%X", b)
			case "TIMESTAMP":
				params[i] = time.Unix(rand.Int63n(time.Now().Unix()-94608000)+94608000, 0)
			default:
				log.Fatalf("Unsupported type %s", c.dataType)
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
	fmt.Fprintf(&buf, `INSERT INTO %s.%s (`, dbName, tableName)
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

	fmt.Println(buf.String())
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

	dbURL := "postgres://root@localhost:26257/test?sslmode=disable"
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		log.Fatal(err)
	}
	parsedURL.Path = "test"

	// Open connection to server and create a database.
	db, dbErr := sql.Open("postgres", parsedURL.String())
	if dbErr != nil {
		log.Fatal(dbErr)
	}

	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(*concurrency + 1)
	db.SetMaxIdleConns(*concurrency + 1)

	rows, err := db.Query("SELECT column_name, data_type, column_default, is_nullable FROM information_schema.columns WHERE table_name = $1", tableName)
	if err != nil {
		log.Fatal(err)
	}

	var cols []col

	defer rows.Close()
	for rows.Next() {
		var col col
		if err := rows.Scan(&col.name, &col.dataType, &col.cdefault, &col.isNullable); err != nil {
			log.Fatal(err)
		}
		if col.cdefault.String == "unique_rowid()" {
			continue
		}
		if strings.HasPrefix(strings.ToLower(col.dataType), "string") {
			col.dataType = "STRING"
		}
		cols = append(cols, col)
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
		workers[i] = newWorker(db, cols, insertStmt)
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
