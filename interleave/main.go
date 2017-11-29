package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/codahale/hdrhistogram"
	_ "github.com/lib/pq"
)

var dbName string
var dbURL string

// Data loading and dumping flags.
var drop = flag.Bool("drop", false, "Drop and recreate the tables.")
var load = flag.Bool("load", false, "Insert fresh data from --seed (deterministic). Note this will insert ADDITIONAL rows into tables if they already exist. Use with --drop.")
var loadfile = flag.String("load-file", "", "Filepath of file generated from --dump to load into database.")
var dumpfile = flag.String("dump-file", "", "If specified, will dump DB contents to this file.")
var interleaved = flag.Bool("interleaved", false, "If true, interleave tables in a hierarchy during --load.")
var dbms = flag.String("dbms", "cockroach", "DBMS used (values: 'cockroach' or 'postgres'). Relevant for --dump.")
var databases = map[string]bool{
	"cockroach": true,
	"postgres":  true,
}
var nMerchants = flag.Int("merchants", 0, "Number of rows in table <merchant> to generate. Use with --load.")
var nProducts = flag.Int("products", 0, "number of rows in table <product> to generate. Use with --load.")
var nVariants = flag.Int("variants", 0, "number of rows in table <variant> to generate. Use with --load.")
var nStores = flag.Int("stores", 0, "Number of rows in table <store> to generate. Use with --load.")

// Benchmark run-related flags.
var run = flag.Bool("run", false, "Run the benchmark.")
var seed = flag.Int64("seed", 42, "Pseudo-random seed used to generate data.")
var concurrency = flag.Int("concurrency", 2*runtime.NumCPU(), "Number of concurrent writers inserting blocks")
var duration = flag.Duration("duration", 0, "The duration to run. If 0, run forever.")
var query = flag.String("query", "SELECT * FROM merchant JOIN product ON m_id=p_m_id", "The query to benchmark. Each completion is 1 ops.")

func usage() {
	fmt.Printf("Usage: %s [OPTIONS] <DB url>\n", os.Args[0])
	fmt.Println("For example:")
	fmt.Printf("\tCockroachDB: %s --run \"postgres://root@localhost:5432/test?sslmode=disable\"\n", os.Args[0])
	fmt.Printf("\tPostgres: %s --run \"postgres://$(whoami)@localhost:26257/test?sslmode=disable\"\n", os.Args[0])
	flag.PrintDefaults()
}

var schemaVar schemaType
var tables = []tableName{
	merchantTable,
	productTable,
	variantTable,
	storeTable,
}

func init() {
	flag.Usage = usage
	flag.Parse()
	// Parse DB url
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}

	// Schema DDL statements initialization.
	initDDL()

	if !databases[*dbms] {
		log.Fatalf("--dbms must either be 'cockroach' or 'postgres'")
	}

	if *dbms == "postgres" && *interleaved {
		log.Fatalf("--dbms=postgres only works with --interleaved=false")
	}

	if *interleaved {
		schemaVar = interleavedSchema
	} else {
		schemaVar = normalSchema
	}

	// Parse db
	dbURL = flag.Args()[0]
}

var stderr = log.New(os.Stderr, "", 0)

func dropTables(db *sql.DB) {
	for _, table := range tables {
		stderr.Printf("Dropping table <%s>...\n", table)
		if _, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE`, table)); err != nil {
			log.Fatalf("Could not drop table <%s>: %v", table, err)
		}
		stderr.Printf("Dropped table <%s>.\n", table)
	}
}

func connectDB() *sql.DB {
	stderr.Printf("connecting to db at %s\n", dbURL)

	// Open connection to DB.
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		log.Fatal(err)
	}
	// Path contains the DB name.
	dbName = parsedURL.Path[1:]
	stderr.Printf("parsed database name for benchmarking: %s\n", dbName)

	db, err := sql.Open("postgres", parsedURL.String())
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(*concurrency + 1)
	db.SetMaxIdleConns(*concurrency + 1)

	return db
}

func main() {
	db := connectDB()
	defer db.Close()

	if *drop {
		dropTables(db)
	}

	if *loadfile != "" {
		// Load data from a dumpfile.
		stderr.Printf("Loading from dump file %s...\n", *loadfile)
		var cmd *exec.Cmd
		if *dbms == "cockroach" {
			cmd = exec.Command("cockroach", "sql", "--insecure", "--database="+dbName)
		} else if *dbms == "postgres" {
			cmd = exec.Command("psql", dbName)
		}

		// Pipe the file content of the dump file.
		in, err := cmd.StdinPipe()
		if err != nil {
			log.Fatal(err)
		}
		stderr.Println("reading sql dump contents")
		content, err := ioutil.ReadFile(*loadfile)
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			stderr.Println("piping sql dump to command")
			if _, err := in.Write(content); err != nil {
				stderr.Println("Importing failed. Did you remember to --drop the tables?")
				log.Fatal(err)
			}
			if err := in.Close(); err != nil {
				log.Fatal(err)
			}
		}()

		stderr.Println("running load command")
		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Printf("%s\n", string(output))
			log.Fatal(err)
		}

		stderr.Printf("Loading from file %s complete!\n", *loadfile)
	} else if *load {
		// Create tables.
		if err := loadSchema(db); err != nil {
			log.Fatal(err)
		}
		if err := generateData(db); err != nil {
			log.Fatal(err)
		}
		if err := applyConstraints(db); err != nil {
			log.Fatal(err)
		}
	}

	// Dump all data to the dumpfile.
	if *dumpfile != "" {
		stderr.Printf("Dumping from <%s> database (Did you remember to set this to the correct --dbms?)\n", *dbms)
		stderr.Printf("Dumping to file %s...\n", *dumpfile)
		var cmd *exec.Cmd
		if *dbms == "cockroach" {
			cmd = exec.Command("cockroach", "dump", "--insecure", dbName)
		} else if *dbms == "postgres" {
			cmd = exec.Command("pg_dump", dbName)
		}

		output, err := cmd.Output()
		if err != nil {
			log.Fatal(err)
		}
		if err = ioutil.WriteFile(*dumpfile, output, 0644); err != nil {
			log.Fatal(err)
		}

		stderr.Printf("Dump to %s complete!\n", *dumpfile)
	}

	if !*run {
		stderr.Println("--run was set to false. Benchmark complete.")
		return
	}

	// Run queries.

	start := time.Now()
	var wg sync.WaitGroup
	workers := make([]*worker, *concurrency)
	for i := range workers {
		workers[i] = newWorker(db, &wg)
		go workers[i].run(&wg)
	}

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
		elapsed := time.Since(start)
		ops := atomic.LoadUint64(&numOps)
		fmt.Printf("%s\t%12.1f ns/op\n",
			"roach-bench", float64(elapsed.Nanoseconds())/float64(ops))
	}()

	cumLatency := hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)

	lastNow := time.Now()
	var lastOps uint64
	for i := 0; ; {
		select {
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
			now := time.Now()
			elapsed := now.Sub(lastNow)
			ops := numOps
			if i%20 == 0 {
				fmt.Println("_time______ops/s(inst)__ops/s(cum)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			i++
			totalTime := time.Duration(time.Since(start).Seconds()+0.5) * time.Second
			fmt.Printf("%5s %12.1f %11.1f %8.1f %8.1f %8.1f %8.1f\n",
				totalTime,
				float64(ops-lastOps)/elapsed.Seconds(),
				float64(ops)/time.Since(start).Seconds(),
				time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
				time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
				time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
				time.Duration(h.ValueAtQuantile(100)).Seconds()*1000)

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

			ops := atomic.LoadUint64(&numOps)
			elapsed := time.Since(start).Seconds()
			fmt.Println("\n_elapsed___________ops_____ops/s(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)__pMax(ms)")
			fmt.Printf("%7.1fs %11d %15.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n\n",
				time.Since(start).Seconds(),
				ops,
				float64(ops)/elapsed,
				time.Duration(cumLatency.Mean()).Seconds()*1000,
				time.Duration(cumLatency.ValueAtQuantile(50)).Seconds()*1000,
				time.Duration(cumLatency.ValueAtQuantile(95)).Seconds()*1000,
				time.Duration(cumLatency.ValueAtQuantile(99)).Seconds()*1000,
				time.Duration(cumLatency.ValueAtQuantile(100)).Seconds()*1000)
			return
		}
	}
}
