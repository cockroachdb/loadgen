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
	"database/sql"
	"flag"
	"fmt"
	"net/url"
	"os"
	"runtime"

	_ "github.com/lib/pq"
)

var concurrency = flag.Int("concurrency", 2*runtime.NumCPU(), "Number of concurrent writers inserting blocks")
var drop = flag.Bool("drop", false, "Drop the database and recreate")
var load = flag.Bool("load", false, "Generate fresh TPCC data. Use with -drop")
var verbose = flag.Bool("v", false, "Print verbose debug output")
var warehouses = flag.Int("warehouses", 1, "number of warehouses for loading")

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <db URL>\n\n", os.Args[0])
	flag.PrintDefaults()
}

func setupDatabase(dbURL string) (*sql.DB, error) {
	if *verbose {
		fmt.Printf("connecting to db: %s\n", dbURL)
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

func main() {
	flag.Usage = usage
	flag.Parse()
	if *verbose {
		fmt.Fprintf(os.Stdout, "Starting TPC-C load generator\n")
	}

	dbURL := "postgresql://root@localhost:26257/tpcc?sslmode=disable"
	if flag.NArg() == 1 {
		dbURL = flag.Arg(0)
	}

	db, err := setupDatabase(dbURL)

	if err != nil {
		fmt.Printf("Setting up database connection failed: %s, continuing assuming database already exists.", err)
	}

	if *drop {
		if _, err := db.Exec("DROP DATABASE tpcc"); err != nil {
			panic(err)
		}
	}

	loadSchema(db)

	if *load {
		generateData(db)
	}
}
