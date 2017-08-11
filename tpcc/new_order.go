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
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/lib/pq"
)

// From the TPCC spec, section 2.4:
//
// The New-Order business transaction consists of entering a complete order
// through a single database transaction. It represents a mid-weight, read-write
// transaction with a high frequency of execution and stringent response time
// requirements to satisfy on-line users. This transaction is the backbone of
// the workload. It is designed to place a variable load on the system to
// reflect on-line database activity as typically found in production
// environments.

type orderItem struct {
	ol_supply_w_id int    // supplying warehouse id
	ol_i_id        int    // item id
	i_name         string // item name
	ol_quantity    int    // order quantity
	s_quantity     int    // stock quantity
	brand_generic  string
	i_price        float64 // item price
	ol_amount      float64 // order amount
	ol_delivery_d  pq.NullTime

	remoteWarehouse bool // internal use - item from a local or remote warehouse?
}

type newOrderData struct {
	// This data must all be returned by the transaction. See 2.4.3.3.
	w_id         int // home warehouse ID
	d_id         int // district id
	c_id         int // customer id
	o_id         int // order id
	o_ol_cnt     int // order line count
	c_last       string
	c_credit     string
	c_discount   float64
	w_tax        float64
	d_tax        float64
	o_entry_d    time.Time
	total_amount float64

	items []orderItem
}

var simError = errors.New("simulated user error")

type newOrder struct{}

var _ tpccTx = newOrder{}

func (_ newOrder) run(db *sql.DB, w_id int) (interface{}, error) {
	d := newOrderData{
		w_id:     w_id,
		d_id:     randInt(1, 10),
		c_id:     randCustomerID(),
		o_ol_cnt: randInt(5, 15),
	}
	d.items = make([]orderItem, d.o_ol_cnt)

	// 2.4.1.4: A fixed 1% of the New-Order transactions are chosen at random to
	// simulate user data entry errors and exercise the performance of rolling
	// back update transactions.
	rollback := rand.Intn(100) == 0

	// all_local tracks whether any of the items were from a remote warehouse.
	all_local := 1
	for i := 0; i < d.o_ol_cnt; i++ {
		item := orderItem{
			// 2.4.1.5.3: order has a quantity [1..10]
			ol_quantity: rand.Intn(10) + 1,
		}
		// 2.4.1.5.1 an order item has a random item number, unless rollback is true
		// and it's the last item in the items list.
		if rollback && i == d.o_ol_cnt-1 {
			item.ol_i_id = -1
		} else {
			item.ol_i_id = randItemID()
		}
		// 2.4.1.5.2: 1% of the time, an item is supplied from a remote warehouse.
		item.remoteWarehouse = rand.Intn(100) == 0
		if item.remoteWarehouse {
			all_local = 0
			item.ol_supply_w_id = rand.Intn(*warehouses)
		} else {
			item.ol_supply_w_id = w_id
		}
		d.items[i] = item
	}

	d.o_entry_d = time.Now()

	err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		// Select the warehouse tax rate.
		if err := tx.QueryRow(
			`SELECT w_tax FROM warehouse WHERE w_id = $1`,
			w_id,
		).Scan(&d.w_tax); err != nil {
			return err
		}

		// Select the district tax rate and next available order number, bumping it.
		var d_next_o_id int
		if err := tx.QueryRow(`
				UPDATE district
				SET d_next_o_id = d_next_o_id + 1
				WHERE d_w_id = $1 AND d_id = $2
				RETURNING d_tax, d_next_o_id`,
			d.w_id, d.d_id,
		).Scan(&d.d_tax, &d_next_o_id); err != nil {
			return err
		}

		d.o_id = d_next_o_id - 1

		// Select the customer's discount, last name and credit.
		if err := tx.QueryRow(`
				SELECT c_discount, c_last, c_credit
				FROM customer
				WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3`,
			d.w_id, d.d_id, d.c_id,
		).Scan(&d.c_discount, &d.c_last, &d.c_credit); err != nil {
			return err
		}

		// Insert row into the orders and new orders table.
		if _, err := tx.Exec(`
				INSERT INTO "order" (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
				VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			d.o_id, d.d_id, d.w_id, d.c_id, d.o_entry_d, d.o_ol_cnt, all_local); err != nil {
			return err
		}
		if _, err := tx.Exec(`
				INSERT INTO new_order (no_o_id, no_d_id, no_w_id) 
				VALUES ($1, $2, $3)`,
			d.o_id, d.d_id, d.w_id); err != nil {
			return err
		}

		selectItem, err := tx.Prepare(`SELECT i_price, i_name, i_data FROM item WHERE i_id=$1`)
		if err != nil {
			return err
		}
		updateStock, err := tx.Prepare(fmt.Sprintf(`
			UPDATE stock
			SET (s_quantity, s_ytd, s_order_cnt, s_remote_cnt) =
				(CASE s_quantity >= $1 + 10 WHEN true THEN s_quantity-$1 ELSE (s_quantity-$1)+91 END,
				 s_ytd + $1,
				 s_order_cnt + 1,
				 s_remote_cnt + (CASE $2::bool WHEN true THEN 1 ELSE 0 END))
			WHERE s_i_id=$3 AND s_w_id=$4
			RETURNING s_dist_%02d, s_data`, d.d_id))
		if err != nil {
			return err
		}
		insertOrderLine, err := tx.Prepare(`
			INSERT INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`)
		if err != nil {
			return err
		}

		var i_data string
		// 2.4.2.2: For each o_ol_cnt item in the order, query the relevant item
		// row, update the stock row to account for the order, and insert a new
		// line into the order_line table to reflect the item on the order.
		for i, item := range d.items {
			if err := selectItem.QueryRow(item.ol_i_id).Scan(&item.i_price, &item.i_name, &i_data); err != nil {
				if rollback && item.ol_i_id < 0 {
					// 2.4.2.3: roll back when we're expecting a rollback due to
					// simulated user error (invalid item id) and we actually
					// can't find the item. The spec requires us to actually go
					// to the database for this, even though we know earlier
					// that the item has an invalid number.
					return simError
				}
				return err
			}

			var dist_info, s_data string
			if err := updateStock.QueryRow(
				item.ol_quantity, item.remoteWarehouse, item.ol_i_id, item.ol_supply_w_id,
			).Scan(&dist_info, &s_data); err != nil {
				return err
			}
			if strings.Contains(s_data, originalString) && strings.Contains(i_data, originalString) {
				item.brand_generic = "B"
			} else {
				item.brand_generic = "G"
			}

			item.ol_amount = float64(item.ol_quantity) * item.i_price
			d.total_amount += item.ol_amount
			if _, err := insertOrderLine.Exec(
				d.o_id, // ol_o_id
				d.d_id,
				d.w_id,
				i+1, // ol_number is a counter over the items in the order.
				item.ol_i_id,
				item.ol_supply_w_id,
				item.ol_quantity,
				item.ol_amount,
				dist_info, // ol_dist_info is set to the contents of s_dist_xx
			); err != nil {
				return err
			}
		}
		// 2.4.2.2: total_amount = sum(OL_AMOUNT) * (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX)
		d.total_amount *= (1 - d.c_discount) * (1 + d.w_tax + d.d_tax)

		return nil
	})
	if err == simError {
		return d, nil
	}
	return d, err
}
