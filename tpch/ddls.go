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

package main

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/pkg/errors"
)

type table int

const (
	nation table = iota
	region
	part
	supplier
	partsupp
	customer
	orders
	lineitem
	numTables
)

var createStmts [numTables]string
var createIndexes = []string{
	"create unique index c_ck on customer (c_custkey asc)",
	"create index c_nk on customer (c_nationkey asc)",
	"create unique index p_pk on part (p_partkey asc)",
	"create unique index s_sk on supplier (s_suppkey asc)",
	"create index s_nk on supplier (s_nationkey asc)",
	"create index ps_pk on partsupp (ps_partkey asc)",
	"create index ps_sk on partsupp (ps_suppkey asc)",
	"create unique index ps_pk_sk on partsupp (ps_partkey asc, ps_suppkey asc)",
	"create unique index ps_sk_pk on partsupp (ps_suppkey asc, ps_partkey asc)",
	"create unique index o_ok on orders (o_orderkey asc)",
	"create index o_ck on orders (o_custkey asc)",
	"create index o_od on orders (o_orderdate asc)",
	"create index l_ok on lineitem (l_orderkey asc)",
	"create index l_pk on lineitem (l_partkey asc)",
	"create index l_sk on lineitem (l_suppkey asc)",
	"create index l_sd on lineitem (l_shipdate asc)",
	"create index l_cd on lineitem (l_commitdate asc)",
	"create index l_rd on lineitem (l_receiptdate asc)",
	"create index l_pk_sk on lineitem (l_partkey asc, l_suppkey asc)",
	"create index l_sk_pk on lineitem (l_suppkey asc, l_partkey asc)",
	"create unique index n_nk on nation (n_nationkey asc)",
	"create index n_rk on nation (n_regionkey asc)",
	"create unique index r_rk on region (r_regionkey asc)",
}

var dropStmts = []string{
	"DROP TABLE IF EXISTS nation CASCADE",
	"DROP TABLE IF EXISTS region CASCADE",
	"DROP TABLE IF EXISTS part CASCADE",
	"DROP TABLE IF EXISTS supplier CASCADE",
	"DROP TABLE IF EXISTS partsupp CASCADE",
	"DROP TABLE IF EXISTS orders CASCADE",
	"DROP TABLE IF EXISTS customer CASCADE",
	"DROP TABLE IF EXISTS lineitem CASCADE",
}

func resolveTableTypeFromFileName(filename string) (table, error) {
	switch strings.Split(filename, ".")[0] {
	case "nation":
		return nation, nil
	case "region":
		return region, nil
	case "part":
		return part, nil
	case "supplier":
		return supplier, nil
	case "partsupp":
		return partsupp, nil
	case "customer":
		return customer, nil
	case "orders":
		return orders, nil
	case "lineitem":
		return lineitem, nil
	default:
		return -1, errors.Errorf("filenames must be of the form tabletype.num.tbl, found: '%s'", filename)
	}
}

func createTablesAndIndexes(db *sql.DB) error {
	createStmts[nation] = `
CREATE TABLE nation  ( n_nationkey  INTEGER NOT NULL,
                       n_name       CHAR(25) NOT NULL,
                       n_regionkey  INTEGER NOT NULL,
                       n_comment    VARCHAR(152))
`

	createStmts[region] = `
CREATE TABLE region  ( r_regionkey  INTEGER NOT NULL,
                       r_name       CHAR(25) NOT NULL,
                       r_comment    VARCHAR(152))
`

	createStmts[part] = `
CREATE TABLE part  ( p_partkey     INTEGER NOT NULL,
                     p_name        VARCHAR(55) NOT NULL,
                     p_mfgr        CHAR(25) NOT NULL,
                     p_brand       CHAR(10) NOT NULL,
                     p_type        VARCHAR(25) NOT NULL,
                     p_size        INTEGER NOT NULL,
                     p_container   CHAR(10) NOT NULL,
                     p_retailprice DECIMAL(15,2) NOT NULL,
                     p_comment     VARCHAR(23) NOT NULL )
`

	createStmts[supplier] = `
CREATE TABLE supplier ( s_suppkey     INTEGER NOT NULL,
                        s_name        CHAR(25) NOT NULL,
                        s_address     VARCHAR(40) NOT NULL,
                        s_nationkey   INTEGER NOT NULL,
                        s_phone       CHAR(15) NOT NULL,
                        s_acctbal     DECIMAL(15,2) NOT NULL,
                        s_comment     VARCHAR(101) NOT NULL)
`

	createStmts[partsupp] = `
CREATE TABLE partsupp ( ps_partkey     INTEGER NOT NULL,
                        ps_suppkey     INTEGER NOT NULL,
                        ps_availqty    INTEGER NOT NULL,
                        ps_supplycost  DECIMAL(15,2)  NOT NULL,
                        ps_comment     VARCHAR(199) NOT NULL )
`

	createStmts[customer] = `
CREATE TABLE customer ( c_custkey     INTEGER NOT NULL,
                        c_name        VARCHAR(25) NOT NULL,
                        c_address     VARCHAR(40) NOT NULL,
                        c_nationkey   INTEGER NOT NULL,
                        c_phone       CHAR(15) NOT NULL,
                        c_acctbal     DECIMAL(15,2)   NOT NULL,
                        c_mktsegment  CHAR(10) NOT NULL,
                        c_comment     VARCHAR(117) NOT NULL)
`

	createStmts[orders] = `
CREATE TABLE orders  ( o_orderkey       INTEGER NOT NULL,
                       o_custkey        INTEGER NOT NULL,
                       o_orderstatus    CHAR(1) NOT NULL,
                       o_totalprice     DECIMAL(15,2) NOT NULL,
                       o_orderdate      DATE NOT NULL,
                       o_orderpriority  CHAR(15) NOT NULL,  
                       o_clerk          CHAR(15) NOT NULL, 
                       o_shippriority   INTEGER NOT NULL,
                       o_comment        VARCHAR(79) NOT NULL)
`

	createStmts[lineitem] = `
CREATE TABLE lineitem ( l_orderkey    INTEGER NOT NULL,
                        l_partkey     INTEGER NOT NULL,
                        l_suppkey     INTEGER NOT NULL,
                        l_linenumber  INTEGER NOT NULL,
                        l_quantity    DECIMAL(15,2) NOT NULL,
                        l_extendedprice  DECIMAL(15,2) NOT NULL,
                        l_discount    DECIMAL(15,2) NOT NULL,
                        l_tax         DECIMAL(15,2) NOT NULL,
                        l_returnflag  CHAR(1) NOT NULL,
                        l_linestatus  CHAR(1) NOT NULL,
                        l_shipdate    DATE NOT NULL,
                        l_commitdate  DATE NOT NULL,
                        l_receiptdate DATE NOT NULL,
                        l_shipinstruct CHAR(25) NOT NULL,
                        l_shipmode     CHAR(10) NOT NULL,
                        l_comment      VARCHAR(44) NOT NULL)
`
	if *verbose {
		fmt.Println("Dropping any existing tables")
	}

	if *drop {
		for _, dropStmt := range dropStmts {
			err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
				_, inErr := db.Exec(dropStmt)
				return inErr
			})
			if err != nil {
				if *verbose {
					fmt.Printf("Failed to create database %s... %s\n",
						dropStmt, err)
				}
				return err
			}
		}
	}

	if *verbose {
		fmt.Println("Finished dropping tables. Creating tables")
	}
	for _, createStmt := range createStmts {
		err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
			_, inErr := db.Exec(createStmt)
			return inErr
		})
		if err != nil {
			if *verbose {
				fmt.Printf("Failed to create database %s... %s\n",
					createStmt, err)
			}
			return err
		}
	}
	if *verbose {
		fmt.Println("Finished creating tables. Creating indexes")
	}

	for _, indexStmt := range createIndexes {
		err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
			_, inErr := db.Exec(indexStmt)
			return inErr
		})
		if err != nil {
			if *verbose {
				fmt.Printf("Failed to create index '%s'\n with error: %s",
					indexStmt, err)
			}

		}
	}

	return nil
}
