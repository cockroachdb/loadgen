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
	"math/rand"
	"sync"
	"time"
)

// These constants are all set by the spec - they're not knobs. Don't change
// them.
const nItems = 100000
const nStock = 100000
const nCustomers = 3000
const originalString = "ORIGINAL"

// District constants
const ytd = 30000.00 // also used by warehouse
const nextOrderID = 3001

// Customer constants
const creditLimit = 50000.00
const balance = -10.00
const ytdPayment = 10.00
const middleName = "OE"
const paymentCount = 1
const deliveryCount = 0
const goodCredit = "GC"
const badCredit = "BC"

func prepare(db *sql.DB, query string) *sql.Stmt {
	stmt, err := db.Prepare(query)
	if err != nil {
		panic(err)
	}
	return stmt
}

func parallelLoad(n int, entityName string, loader func(int)) {
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
	stmtItem := prepare(db, `
INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data)
VALUES ($1, $2, $3, $4, $5)`)
	stmtWarehouse := prepare(db, `
INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`)
	stmtStock := prepare(db, `
INSERT INTO stock (
	s_i_id, s_w_id, s_quantity,
	s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10,
	s_ytd, s_order_cnt, s_remote_cnt, s_data)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`)
	stmtDistrict := prepare(db, `
INSERT INTO district (
	d_id, d_w_id, d_name, d_street_1, d_street_2,
	d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`)
	stmtCustomer := prepare(db, `
INSERT INTO customer (
	c_id, c_d_id, c_w_id, c_first, c_middle, c_last,
	c_street_1, c_street_2, c_city, c_state, c_zip,
	c_phone, c_since, c_credit, c_credit_lim, c_discount,
	c_balance, c_ytd_payment, c_payment_cnt,
	c_delivery_cnt, c_data)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)`)
	stmtHistory := prepare(db, `
INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`)
	stmtOrder := prepare(db, `
INSERT INTO "order" (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`)
	stmtOrderLine := prepare(db, `
INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`)
	stmtNewOrder := prepare(db, `
INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
VALUES ($1, $2, $3)`)

	// See section 1.3 for the general layout of the tables.
	// See section 4.3 for the rules on how to populate the database.

	fmt.Println("Loading items...")

	// 100,000 items.
	// TODO(jordan): send these inserts in batches.
	parallelLoad(nItems, "item", func(id int) {
		if _, err := stmtItem.Exec(id,
			randInt(1, 10000),                         // im_id: "Image ID associated to Item"
			randAString(14, 24),                       // name
			float64(randInt(100, 10000))/float64(100), // price
			randOriginalString(),
		); err != nil {
			panic(err)
		}
	})

	now := time.Now()
	for wID := 0; wID < *warehouses; wID++ {
		fmt.Printf("Loading warehouse %d...\n", wID)
		if _, err := stmtWarehouse.Exec(wID,
			randInt(6, 10),  // name
			randInt(10, 20), // street_1
			randInt(10, 20), // street_2
			randInt(10, 20), // city
			randState(),
			randZip(),
			randTax(),
			ytd); err != nil {
			panic(err)
		}

		// 100,000 stock per warehouse.
		parallelLoad(nStock, "stock", func(id int) {
			if _, err := stmtStock.Exec(id, wID,
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
		})

		// 10 districts per warehouse
		for dID := 1; dID <= 10; dID++ {
			fmt.Printf("Loading warehouse %d district %d...\n", wID, dID)
			if _, err := stmtDistrict.Exec(dID, wID,
				randAString(6, 10),  // name
				randAString(10, 20), // street 1
				randAString(10, 20), // street 2
				randAString(10, 20), // city
				randState(),
				randZip(),
				randTax(),
				ytd,
				nextOrderID); err != nil {
				panic(err)
			}

			// 3000 customers per district
			parallelLoad(nCustomers, "customer", func(cID int) {
				// 10% of the customer rows have bad credit.
				// See section 4.3, under the CUSTOMER table population section.
				credit := goodCredit
				if rand.Intn(9) == 0 {
					// Poor 10% :(
					credit = badCredit
				}
				var lastName string
				// The first 1000 customers get a last name generated according to their id;
				// the rest get an NURand generated last name.
				if cID <= 1000 {
					lastName = randCLastSyllables(cID - 1)
				} else {
					lastName = randCLast()
				}
				if _, err := stmtCustomer.Exec(cID, dID, wID,
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
				if _, err := stmtHistory.Exec(cID, dID, wID, dID, wID, time.Now(), 10.00, randAString(12, 24)); err != nil {
					panic(err)
				}
			})

			// 3000 orders per district, with a random permutation over the customers.
			var randomCIDs [nCustomers]int
			for i, cID := range rand.Perm(nCustomers) {
				randomCIDs[i] = cID + 1
			}
			parallelLoad(nCustomers, "order", func(oID int) {
				olCnt := randInt(5, 15)
				var carrierID interface{}
				if oID < 2101 {
					carrierID = randInt(1, 10)
				} else {
					carrierID = sql.NullInt64{Int64: 0, Valid: false}
				}
				entryD := time.Now()
				if _, err := stmtOrder.Exec(oID, dID, wID, randomCIDs[oID-1], entryD, carrierID, olCnt, 1); err != nil {
					panic(err)
				}

				for olNumber := 1; olNumber <= olCnt; olNumber++ {
					var amount float64
					var deliveryD interface{}
					if oID < 2101 {
						amount = 0
						deliveryD = entryD
					} else {
						amount = float64(randInt(1, 999999)) / 100.0
						deliveryD = sql.NullInt64{Int64: 0, Valid: false}
					}

					if _, err := stmtOrderLine.Exec(oID, dID, wID, olNumber,
						randInt(1, 100000), // ol_i_id
						wID,                // supply_w_id
						deliveryD,
						5, // quantity
						amount,
						randAString(24, 24)); err != nil {
						panic(err)
					}
				}

				// The last 900 orders have entries in new orders.
				if oID >= 2101 {
					if _, err := stmtNewOrder.Exec(oID, dID, wID); err != nil {
						panic(err)
					}
				}
			})
			fmt.Printf("Loaded warehouse %d district %d\n", wID, dID)
		}
	}
}
