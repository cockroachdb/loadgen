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
	"fmt"
	"io/ioutil"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
)

// These constants are all sent by the spec - they're not knobs. Don't change
// them.
const nItems = 100000
const nStock = 100000
const nCustomers = 3000

// District constants
const ytd = 30000.00 // also used by warehouse
const nextOrderId = 3001

// Customer constants
const creditLimit = 50000.00
const balance = -10.00
const ytdPayment = 10.00
const middleName = "OE"
const paymentCount = 1
const deliveryCount = 0
const goodCredit = "GC"
const badCredit = "BC"

// loadSchema loads the entire TPCC schema into the database.
func loadSchema(db *sql.DB) {
	data, err := ioutil.ReadFile("ddls.sql")
	if err != nil {
		panic(err)
	}
	stmts := strings.Split(string(data), ";")
	for _, stmt := range stmts {
		if err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
			_, inErr := tx.Exec(stmt)
			return inErr
		}); err != nil {
			panic(err)
		}
	}
}

func prepare(db *sql.DB, query string) *sql.Stmt {
	stmt, err := db.Prepare(query)
	if err != nil {
		panic(err)
	}
	return stmt
}

func startWorkerPool(loader func(int), n int, entityName string) {
	var wg sync.WaitGroup
	ch := make(chan int)
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range ch {
				loader(id)
			}
		}()
	}

	for id := 1; id <= n; id++ {
		ch <- id
		if id%1000 == 0 {
			fmt.Printf("Loaded %d/%d %ss\n", id, n, entityName)
		}
	}
	close(ch)
	wg.Wait()
}

func generateData(db *sql.DB) {
	stmtItem := prepare(db, `INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data) VALUES ($1, $2, $3, $4, $5)`)
	stmtWarehouse := prepare(db, `INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`)
	stmtStock := prepare(db, `INSERT INTO stock (s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`)
	stmtDistrict := prepare(db, `INSERT INTO district (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`)
	stmtCustomer := prepare(db, `INSERT INTO customer (
				  c_id, c_d_id, c_w_id, c_first, c_middle, c_last,
				  c_street_1, c_street_2, c_city, c_state, c_zip,
				  c_phone, c_since, c_credit, c_credit_lim, c_discount,
				  c_balance, c_ytd_payment, c_payment_cnt,
				  c_delivery_cnt, c_data)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
				        $16, $17, $18, $19, $20, $21)`)
	stmtHistory := prepare(db, `INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`)
	stmtOrder := prepare(db, `INSERT INTO "order" (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)  VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`)
	stmtOrderLine := prepare(db, `INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`)
	stmtNewOrder := prepare(db, `INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES ($1, $2, $3)`)

	fmt.Println("Loading items...")

	// 100,000 items.
	startWorkerPool(func(id int) {
		if _, err := stmtItem.Exec(id, randInt(1, 10000), randAString(14, 24), float64(randInt(100, 10000))/float64(100), randOriginalString()); err != nil {
			panic(err)
		}
	}, nItems, "item")

	now := time.Now()
	for w_id := 0; w_id < *warehouses; w_id++ {
		fmt.Printf("Loading warehouse %d...\n", w_id)
		stmtWarehouse.Exec(w_id,
			randInt(6, 10),  // name
			randInt(10, 20), // street_1
			randInt(10, 20), // street_2
			randInt(10, 20), // city
			randState(),
			randZip(),
			randTax(),
			ytd)

		// 100,000 stock per warehouse..
		startWorkerPool(func(id int) {
			if _, err := stmtStock.Exec(id, w_id,
				randInt(10, 100),     // quantity
				randAString(24, 24),  // dist_01
				randAString(24, 24),  // dist_02
				randAString(24, 24),  // dist_03
				randAString(24, 24),  // dist_04
				randAString(24, 24),  // dist_05
				randAString(24, 24),  // dist_06
				randAString(24, 24),  // dist_07
				randAString(24, 24),  // dist_08
				randAString(24, 24),  // dist_09
				randAString(24, 24),  // dist_10
				0,                    // ytd
				0,                    // order_cnt
				0,                    // remote_cnt
				randOriginalString(), // data
			); err != nil {
				panic(err)
			}
		}, nStock, "stock")

		// 10 districts per warehouse
		for d_id := 1; d_id <= 10; d_id++ {
			fmt.Printf("Loading warehouse %d district %d...\n", w_id, d_id)
			stmtDistrict.Exec(d_id, w_id,
				randAString(6, 10),  // name
				randAString(10, 20), // street 1
				randAString(10, 20), // street 2
				randAString(10, 20), // city
				randState(),
				randZip(),
				randTax(),
				ytd,
				nextOrderId)

			// 3000 customers per district
			startWorkerPool(func(c_id int) {
				// 10% of the customer rows have bad credit.
				credit := goodCredit
				if rand.Intn(9) == 0 {
					// Poor 10% :(
					credit = badCredit
				}
				var lastName string
				// The first 1000 customers get a last name generated according to their id;
				// the rest get an NURand generated last name.
				if c_id <= 1000 {
					lastName = randCLastSyllables(c_id - 1)
				} else {
					lastName = randCLast()
				}
				if _, err := stmtCustomer.Exec(c_id, d_id, w_id,
					randAString(8, 16), // first name
					middleName,
					lastName,
					randAString(10, 20), // street 1
					randAString(10, 20), // street 2
					randAString(10, 20), // city name
					randState(),
					randZip(),
					randNString(16, 16), // phone number
					now,
					credit,
					creditLimit,
					float64(randInt(0, 5000))/float64(10000.0), // discount
					balance,
					ytdPayment,
					paymentCount,
					deliveryCount,
					randAString(300, 500), // data
				); err != nil {
					panic(err)
				}

				// 1 history row per customer
				if _, err := stmtHistory.Exec(c_id, d_id, w_id, d_id, w_id, time.Now(), 10.00, randAString(12, 24)); err != nil {
					panic(err)
				}
			}, nCustomers, "customer")

			// 3000 orders per district, with a random permutation over the customers.
			var randomCIDs [nCustomers]int
			for i, c_id := range rand.Perm(nCustomers) {
				randomCIDs[i] = c_id + 1
			}
			startWorkerPool(func(o_id int) {
				ol_cnt := randInt(5, 15)
				var carrierId interface{}
				if o_id < 2101 {
					carrierId = randInt(1, 10)
				} else {
					carrierId = sql.NullInt64{Int64: 0, Valid: false}
				}
				entry_d := time.Now()
				if _, err := stmtOrder.Exec(o_id, d_id, w_id, randomCIDs[o_id-1], entry_d, carrierId, ol_cnt, 1); err != nil {
					panic(err)
				}

				for ol_number := 1; ol_number <= ol_cnt; ol_number++ {
					var amount float64
					var delivery_d interface{}
					if o_id < 2101 {
						amount = 0
						delivery_d = entry_d
					} else {
						amount = float64(randInt(1, 999999)) / 100.0
						delivery_d = sql.NullInt64{Int64: 0, Valid: false}
					}

					if _, err := stmtOrderLine.Exec(o_id, d_id, w_id, ol_number,
						randInt(1, 100000), // ol_i_id
						w_id,               // supply_w_id
						delivery_d,
						5, // quantity
						amount,
						randAString(24, 24)); err != nil {
						panic(err)
					}
				}

				// The last 900 orders have entries in new orders.
				if o_id >= 2101 {
					if _, err := stmtNewOrder.Exec(o_id, d_id, w_id); err != nil {
						panic(err)
					}
				}
			}, nCustomers, "order")
			fmt.Printf("Loaded warehouse %d district %d\n", w_id, d_id)
		}
	}
}
