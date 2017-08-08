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
	"math/rand"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach-go/crdb"
)

// From the TPCC spec, section 2.6:
//
// The Order-Status business transaction queries the status of a customer's last
// order. It represents a mid-weight read-only database transaction with a low
// frequency of execution and response time requirement to satisfy on-line
// users. In addition, this table includes non-primary key access to the
// CUSTOMER table.

type orderStatusData struct {
	// Return data specified by 2.6.3.3
	d_id         int
	c_id         int
	c_first      string
	c_middle     string
	c_last       string
	c_balance    float64
	o_id         int
	o_entry_d    time.Time
	o_carrier_id sql.NullInt64

	items []orderItem
}

type customerData struct {
	c_id      int
	c_balance float64
	c_first   string
	c_middle  string
}

type orderStatus struct{}

var _ tpccTx = orderStatus{}

func (_ orderStatus) weight() int {
	return orderStatusWeight
}

func (_ orderStatus) run(db *sql.DB, w_id int) (interface{}, error) {
	d := orderStatusData{
		d_id: rand.Intn(9) + 1,
	}

	// 2.6.1.2: The customer is randomly selected 60% of the time by last name
	// and 40% by number.
	if rand.Intn(9) < 6 {
		d.c_last = randCLast()
	} else {
		d.c_id = randCustomerID()
	}

	if err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		// 2.6.2.2 explains this entire transaction.

		// Select the customer
		if d.c_id != 0 {
			// Case 1: select by customer id
			if err := tx.QueryRow(`
					SELECT c_balance, c_first, c_middle, c_last
					FROM customer
					WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3`,
				w_id, d.d_id, d.c_id,
			).Scan(&d.c_balance, &d.c_first, &d.c_middle, &d.c_last); err != nil {
				return errors.Wrap(err, "select by customer idfail")
			}
		} else {
			// Case 2: Pick the middle row, rounded up, from the selection by last name.
			rows, err := tx.Query(`
					SELECT c_id, c_balance, c_first, c_middle
					FROM customer
					WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3
					ORDER BY c_first ASC`,
				w_id, d.d_id, d.c_last)
			if err != nil {
				return errors.Wrap(err, "select by last name fail")
			}
			customers := make([]customerData, 0, 1)
			for rows.Next() {
				c := customerData{}
				err = rows.Scan(&c.c_id, &c.c_balance, &c.c_first, &c.c_middle)
				if err != nil {
					rows.Close()
					return err
				}
				customers = append(customers, c)
			}
			rows.Close()
			cIdx := len(customers) / 2
			if len(customers)%2 == 0 {
				cIdx -= 1
			}

			c := customers[cIdx]
			d.c_id = c.c_id
			d.c_balance = c.c_balance
			d.c_first = c.c_first
			d.c_middle = c.c_middle
		}

		// Select the customer's order.
		if err := tx.QueryRow(`
				SELECT o_id, o_entry_d, o_carrier_id
				FROM "order"
				WHERE o_w_id = $1 AND o_d_id = $2 AND o_c_id = $3
				ORDER BY o_id DESC
				LIMIT 1`,
			w_id, d.d_id, d.c_id,
		).Scan(&d.o_id, &d.o_entry_d, &d.o_carrier_id); err != nil {
			return errors.Wrap(err, "select order fail")
		}

		// Select the items from the customer's order.
		rows, err := tx.Query(`
				SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
				FROM order_line
				WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3`,
			w_id, d.d_id, d.o_id)
		if err != nil {
			return errors.Wrap(err, "select items fail")
		}
		defer rows.Close()

		// On average there's 10 items per order - 2.4.1.3
		d.items = make([]orderItem, 0, 10)
		for rows.Next() {
			item := orderItem{}
			if err := rows.Scan(&item.ol_i_id, &item.ol_supply_w_id, &item.ol_quantity, &item.ol_amount, &item.ol_delivery_d); err != nil {
				return err
			}
			d.items = append(d.items, item)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return d, nil
}
