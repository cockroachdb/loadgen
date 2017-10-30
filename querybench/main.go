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

package main

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/codahale/hdrhistogram"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

var concurrency = flag.Int("concurrency", 1, "Number of workers looping over queries concurrently")
var dbURL = flag.String("dburl", "", "Database connection string to use")
var distsql = flag.Bool("dist-sql", true, "Use DistSQL for query execution (this sets the cluster setting)")
var loops = flag.Uint("loops", 0, "Number of times to run the queries (0 = run forever)")
var histMax = flag.Duration("hist-max", 5*time.Minute, "Maximum time a query is expected to take (used as a histogram cutoff)")
var histMin = flag.Duration("hist-min", 500*time.Millisecond, "Minimum time a query is expected to take (used as a histogram cutoff)")
var queryFile = flag.String("query-file", "", "File of newline separated queries to run")
var tolerateErrors = flag.Bool("tolerate-errors", false, "Keep running on error")
var verbose = flag.Bool("v", false, "Print verbose debug output")

type sharedHistogram struct {
	sync.Mutex
	*hdrhistogram.Histogram
	numOps uint64
}

// setupDatabase performs initial setup.
func setupDatabase(dbURL string) (*sql.DB, error) {
	if *verbose {
		log.Printf("connecting to %s\n", dbURL)
	}
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}

	// Open connection to server.
	db, err := sql.Open("postgres", parsedURL.String())
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(*concurrency + 1)
	db.SetMaxIdleConns(*concurrency + 1)

	return db, nil
}

// getFileLines returns the lines of a file as a string slice. Ignores lines
// beginning with '#'.
func getFileLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Println(errors.Wrapf(err, "error closing %s", path))
		}
	}()

	scanner := bufio.NewScanner(file)
	var lines []string
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) < 1 || line[0] == '#' {
			continue
		}
		lines = append(lines, scanner.Text())
	}
	return lines, nil
}

// cleanQuerySlice adds a semicolon to the end of each query if not present.
func cleanQuerySlice(queries []string) {
	for i, query := range queries {
		if query[len(query)-1] != ';' {
			queries[i] = string(append([]byte(query), ';'))
		}
	}
}

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s [<db URL>]\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "If the numbers reported seem weird (e.g. all percentiles are the same) set -{max,min}-latency\n\n")
	flag.PrintDefaults()
}

// loopQueries runs the given list of queries *loops times (0 runs the queries
// forever).
func loopQueries(ctx context.Context, wg *sync.WaitGroup, errChan chan<- error, id int, db *sql.DB, queries []string, histograms []*sharedHistogram) {
	defer wg.Done()
	for i := uint(0); i < *loops || *loops == 0; i++ {
		for j, query := range queries {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if *verbose {
				log.Printf("worker [%d] running query %s\n", id, query)
			}
			start := time.Now()
			rows, err := db.Query(query)
			if err != nil {
				errChan <- errors.Wrapf(err, "error running query %s", query)
			}
			// rows.Close() must be called to release the db connection so that
			// it can be reused on the next iteration (although note that we
			// might not necessarily reuse the same connection).
			if err := rows.Close(); err != nil {
				errChan <- errors.Wrapf(err, "error closing results")
			}
			elapsed := time.Since(start)

			histograms[j].Lock()
			if err := histograms[j].RecordValue(elapsed.Nanoseconds()); err != nil {
				errChan <- errors.Wrapf(err, "error closing results")
			}
			histograms[j].numOps++
			histograms[j].Unlock()
			if *verbose {
				log.Printf("worker [%d] ran query %s in %s\n", id, query, elapsed)
			}
		}
	}
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() > 1 {
		flag.Usage()
		os.Exit(1)
	}

	if *queryFile == "" {
		fmt.Println("query file path not specified")
		os.Exit(1)
	}

	dbURL := fmt.Sprintf("postgresql://root@localhost:26257?sslmode=disable")
	if flag.NArg() == 1 {
		dbURL = flag.Arg(0)
	} else if *verbose {
		// No database argument.
		fmt.Println("no database url specified, make sure queries specify a database")
	}

	db, err := setupDatabase(dbURL)
	if err != nil {
		log.Fatal(err)
	}

	distsqlSetting := "off"
	if *distsql {
		distsqlSetting = "on"
	}
	// TODO(asubiotto): This is fine for now, but we might want to set the
	// setting per-session. The issue is that we don't know what connection
	// database/sql will give back to us and therefore cannot be sure that
	// setting the distsql setting *concurrency times (once per worker) will
	// result in all connections having that distsql setting.
	if _, err := db.Exec(fmt.Sprintf("SET CLUSTER SETTING sql.defaults.distsql = %s;", distsqlSetting)); err != nil {
		log.Fatal(err)
	}

	queries, err := getFileLines(*queryFile)
	if err != nil {
		log.Fatal(err)
	}
	if len(queries) < 1 {
		fmt.Println("please provide queries in your file")
		os.Exit(1)
	}
	cleanQuerySlice(queries)

	histograms := make([]*sharedHistogram, len(queries))
	for i := range histograms {
		histograms[i] = &sharedHistogram{Histogram: hdrhistogram.New(histMin.Nanoseconds(), histMax.Nanoseconds(), 1 /* sigfig */)}
	}

	ctx, cancelWorkers := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	errChan := make(chan error)
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go loopQueries(ctx, &wg, errChan, i, db, queries, histograms)
	}

	done := make(chan os.Signal, 3)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		wg.Wait()
		done <- syscall.Signal(syscall.SIGTERM)
	}()

	for {
		select {
		case err := <-errChan:
			if !*tolerateErrors {
				log.Fatal(err)
			}
			log.Println(err)
		case <-done:
			if *verbose {
				log.Println("canceling workers and printing results")
			}
			cancelWorkers()
			// For pretty results.
			maxLen := 0
			for _, query := range queries {
				if len(query) > maxLen {
					maxLen = len(query)
				}
			}
			// Width of information (i.e. latencies and headers for these).
			histograms[0].Lock()
			infoLen := len(time.Duration(histograms[0].ValueAtQuantile(100)).String())
			if infoLen < 6 {
				infoLen = 6
			}
			histograms[0].Unlock()
			printLatencies := func(query, numOps, p50, p95, p99, p100 string) {
				fmt.Printf(
					"%[1]*[3]s %[2]*[4]s %[2]*[5]s %[2]*[6]s %[2]*[7]s %[2]*[8]s\n",
					maxLen,
					infoLen,
					query,
					numOps,
					p50,
					p95,
					p99,
					p100,
				)
			}
			printLatencies("Query", "NumOps", "p50", "p95", "p99", "p100")
			for i, query := range queries {
				histograms[i].Lock()
				printLatencies(
					query,
					fmt.Sprintf("%d", histograms[i].numOps),
					time.Duration(histograms[i].ValueAtQuantile(50)).String(),
					time.Duration(histograms[i].ValueAtQuantile(95)).String(),
					time.Duration(histograms[i].ValueAtQuantile(99)).String(),
					time.Duration(histograms[i].ValueAtQuantile(100)).String(),
				)
				histograms[i].Unlock()
			}
			return
		}
	}
}
