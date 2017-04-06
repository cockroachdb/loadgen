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
// The TPC-H  program is intended to simulate the workload specified by
// the Transaction Processing Council Benchmark TPC-H

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"sync"
)

var verbose = flag.Bool("v", false, "Print verbose debug output")
var drop = flag.Bool("drop", false,
	"Drop the existing table and recreate it to start from scratch")
var load = flag.Bool("load", false,
	"Load data into the database from ")
var restore = flag.String("restore", "",
	"Restore data from the specified backup file. Cannot be used with load.")
var dataDir = flag.String("data-dir", "data",
	"Source for data files to load from. Data must be generated using the DBGEN utility.")
var distsql = flag.Bool("dist-sql", true, "Use DistSQL for query execution (default true)")
var scaleFactor = flag.Uint("scale-factor", 1, "The Scale Factor for the TPC-H benchmark")
var insertsPerTransaction = flag.Uint("inserts-per-tx", 100, "Number of inserts to batch into a single transaction when loading data")
var queries = flag.String("queries", "1,3,7,8,9,19", "Queries to run. Use a comma separated list of query numbers. Default: (1,3,7,8,9,19)")
var loops = flag.Uint("loops", 1, "Number of times to run the queries (0 = run forever).")
var concurrency = flag.Uint("concurrency", 1, "Number of queries to execute concurrently.")
var maxErrors = flag.Uint64("max-errors", 1, "Number of query errors allowed before aborting (0 = unlimited).")

// Flags for testing this load generator.
var insertLimit = flag.Uint("insert-limit", 0, "Limit number of rows to be inserted from each file "+
	"(0 = unlimited")

func loadFile(dbURL string, datafile string, t table) error {
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return errors.Wrap(err, "error parsing DB URL")
	}

	// Open connection to server
	db, err := sql.Open("postgres", parsedURL.String())
	if err != nil {
		return errors.Wrap(err, "database connection error")
	}

	filename := fmt.Sprintf("%s/%s", "data", datafile)
	if err := insertTableFromFile(db, filename, t); err != nil {
		return errors.Wrap(err, "table insertion error")
	}
	return nil
}

func runRestore(db *sql.DB, restoreLoc string) error {
	restoreStmt := fmt.Sprintf("RESTORE tpch.* FROM '%s'", restoreLoc)
	_, err := db.Exec(restoreStmt)
	return err
}

// setupDatabase performs initial setup for the example, creating a database
// with a single table. If the desired table already exists on the cluster, the
// existing table will be dropped if the -drop flag was specified.
func setupDatabase(dbURL string) (*sql.DB, error) {
	if *verbose {
		log.Printf("connecting to db: %s\n", dbURL)
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

	return db, nil
}

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <db URL>\n\n", os.Args[0])
	flag.PrintDefaults()
}

// loopQueries runs the given list of queries *loops times. id is used for
// logging. Query errors are atomically added to errorCount, resulting in a
// fatal error if we have more than *maxErrors errors.
func loopQueries(id uint, db *sql.DB, queries []int, wg *sync.WaitGroup, errorCount *uint64) {
	for i := uint(0); i < *loops || *loops == 0; i++ {
		for _, query := range queries {
			if *verbose {
				log.Printf("[%d] running query %d", id, query)
			}
			start := time.Now()
			numRows, err := runQuery(db, query)
			elapsed := time.Now().Sub(start)
			if err != nil {
				newErrorCount := atomic.AddUint64(errorCount, 1)
				wrappedErr := errors.Wrapf(err, "[%d] error running query %d", id, query)
				if newErrorCount < *maxErrors || *maxErrors == 0 {
					log.Print(wrappedErr)
				} else {
					log.Fatal(wrappedErr)
				}
				return
			}
			log.Printf("[%d] finished query %d: %d rows returned after %4.2f seconds\n",
				id, query, numRows, elapsed.Seconds())
		}
	}
	wg.Done()
}

func main() {
	flag.Usage = usage
	flag.Parse()
	if *verbose {
		fmt.Fprintf(os.Stdout, "Starting TPC-H load generator\n")
	}

	dbURL := "postgresql://root@localhost:26257/tpch?sslmode=disable"
	if flag.NArg() == 1 {
		dbURL = flag.Arg(0)
	}

	db, err := setupDatabase(dbURL)

	if err != nil {
		fmt.Printf("Setting up database connection failed: %s, continuing assuming database already exists.", err)
	}

	if (*restore != "") && *load {
		log.Fatal("only one of -load or -restore must be specified.")
	}

	// Ensure the database exists
	if err = crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		_, inErr := tx.Exec("CREATE DATABASE IF NOT EXISTS tpch")
		return inErr
	}); err != nil {
		if *verbose {
			log.Fatalf("failed to create database: %s\n", err)
		}
	}

	if *restore != "" {
		if err = runRestore(db, *restore); err != nil {
			log.Fatalf("restore failed: %s", err)
		}
	}

	if *load {
		if err = createTables(db); err != nil {
			log.Fatalf("creating tables and indices failed: %s\n", err)
		}

		if *verbose {
			log.Printf("database setup complete. Loading...\n")
		}

		files, err := ioutil.ReadDir("./data/")
		if err != nil {
			log.Fatalf("failed to read data directory for loading data: %s", err)
		}

		loadStart := time.Now()
		for _, file := range files {
			t, err := resolveTableTypeFromFileName(file.Name())
			if err != nil {
				log.Fatal(err)
			}
			if err := loadFile(dbURL, file.Name(), t); err != nil {
				log.Fatal(err)
			}
		}

		if err := createIndexes(db); err != nil {
			log.Fatal("failed to create indexes: ", err)
		}

		if *verbose {
			log.Printf("loading complete, total time elapsed: %s\n",
				time.Since(loadStart))
		}
	}

	// Create *concurrency goroutines, each looping over queries in *queries.
	listQueries := strings.Split(*queries, ",")
	var queries []int
	for _, query := range listQueries {
		query = strings.TrimSpace(query)
		queryInt, err := strconv.Atoi(query)
		if err != nil {
			log.Fatalf("error: query %s must be an integer", query)
		}
		queries = append(queries, queryInt)
	}
	var wg sync.WaitGroup
	var errorCount uint64
	for i := uint(0); i < *concurrency; i++ {
		wg.Add(1)
		go loopQueries(i, db, queries, &wg, &errorCount)
	}
	wg.Wait()
}
