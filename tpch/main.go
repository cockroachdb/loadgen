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
	"runtime"
	"strings"
	"sync"
	"time"

	"strconv"

	"github.com/cockroachdb/cockroach-go/crdb"
	_ "github.com/lib/pq"
)

var concurrency = flag.Int("load-concurrency", 2*runtime.NumCPU(),
	"Number of concurrent loaders populating the initial tables.")
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

func runLoader(dbURL string, datafile string, t table, wg *sync.WaitGroup) {
	defer func() {
		if *verbose {
			fmt.Printf("Worker done loading from file '%s'\n", datafile)
		}
		wg.Done()
	}()

	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		fmt.Printf("Error encountered in parsing DB URL: %s\n", err)
	}

	// Open connection to server
	db, err := sql.Open("postgres", parsedURL.String())
	if err != nil {
		fmt.Printf("Error encountered in connecting to database: %s\n", err)
	}

	filename := fmt.Sprintf("%s/%s", "data", datafile)
	if err := insertTableFromFile(db, filename, t); err != nil {
		fmt.Printf("Error encountered in table insertion: %s\n", err)
	}
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
		fmt.Printf("Setting up the database. Connecting to db: %s\n", dbURL)
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
		log.Fatal("Only one of -load or -restore must be specified.")
	}

	// Ensure the database exists
	if err = crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		_, inErr := tx.Exec("CREATE DATABASE IF NOT EXISTS tpch")
		return inErr
	}); err != nil {
		if *verbose {
			log.Fatalf("Failed to create the %s\n",
				err)
		}
	}

	if *restore != "" {
		if err = runRestore(db, *restore); err != nil {
			log.Fatalf("Restore failed: %s", err)
		}
	}

	if *load {
		if err = createTables(db); err != nil {
			fmt.Printf("Creating tables and indices failed: %s\n", err)
		}

		if *verbose {
			fmt.Printf("Database setup complete. Loading...\n")
		}

		files, err := ioutil.ReadDir("./data/")
		if err != nil {
			log.Fatalf("Failed to read data directory for loading data: %s", err)
		}

		loadStart := time.Now()
		var wg sync.WaitGroup
		for _, file := range files {
			wg.Add(1)
			t, err := resolveTableTypeFromFileName(file.Name())
			if err != nil {
				log.Fatal(err)
			}
			runLoader(dbURL, file.Name(), t, &wg)
		}
		wg.Wait()
		if *verbose {
			fmt.Printf("Loading complete, total time elapsed: %s\n",
				time.Since(loadStart))
		}
	}

	listQueries := strings.Split(*queries, ",")
	for _, query := range listQueries {
		start := time.Now()
		query = strings.TrimSpace(query)
		queryInt, err := strconv.Atoi(query)
		if err != nil {
			fmt.Printf("Error: Query %s must be an integer", query)
		}
		numRows, err := runQuery(db, queryInt)
		elapsed := time.Now().Sub(start)
		if err != nil {
			fmt.Printf("Error occured when running query %s: %s\n", query, err)
		}
		fmt.Printf("Finished query %s: %d rows returned after %4.2f seconds\n",
			query, numRows, elapsed.Seconds())
	}
}
