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
)

var queryStmts = [...]string{
	1:  query1,
	2:  query2,
	3:  query3,
	4:  query4,
	5:  query5,
	6:  query6,
	7:  query7,
	8:  query8,
	9:  query9,
	10: query10,
	11: query11,
	12: query12,
	13: query13,
	14: query14,
	15: query15,
	16: query16,
	17: query17,
	18: query18,
	19: query19,
	20: query20,
	21: query21,
	22: query22,
}

func runQuery(db *sql.DB, query int) (int, error) {
	var queryString string
	if *distsql {
		queryString = "SET DISTSQL = 'always'; "
	}
	queryString = fmt.Sprintf("%s%s", queryString, queryStmts[query])

	switch query {
	case 2, 4, 13, 16, 17, 18, 20, 21, 22:
		fmt.Println("Warning: query is unsupported")
	case 5, 6, 10, 12, 14:
		fmt.Println("Warning: query causes Cockroach to panic (see #13692), not running query.")
		return 0, nil
	case 11:
		fmt.Println("Warning: group with having not supported yet")
	}

	rows, err := db.Query(queryString)
	if err != nil {
		return 0, err
	}
	var rowCount int
	for rows.Next() {
		rowCount++
	}

	return rowCount, nil
}
