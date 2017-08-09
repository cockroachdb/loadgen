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
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/pkg/errors"
)

// Section 2.5:
//
// The Payment business transaction updates the customer's balance and reflects
// the payment on the district and warehouse sales statistics. It represents a
// light-weight, read-write transaction with a high frequency of execution and
// stringent response time requirements to satisfy on-line users. In addition,
// this transaction includes non-primary key access to the CUSTOMER table.

type paymentData struct {
	// This data must all be returned by the transaction. See 2.5.3.4.
	d_id   int
	c_id   int
	c_d_id int
	c_w_id int

	w_street_1 string
	w_street_2 string
	w_city     string
	w_state    string
	w_zip      string

	d_street_1 string
	d_street_2 string
	d_city     string
	d_state    string
	d_zip      string

	c_first      string
	c_middle     string
	c_last       string
	c_street_1   string
	c_street_2   string
	c_city       string
	c_state      string
	c_zip        string
	c_phone      string
	c_since      time.Time
	c_credit     string
	c_credit_lim float64
	c_discount   float64
	c_balance    float64
	c_data       string

	h_amount float64
	h_date   time.Time
}

type payment struct{}

var _ tpccTx = payment{}

func (_ payment) run(db *sql.DB, w_id int) (interface{}, error) {
	d := paymentData{
		d_id: rand.Intn(10) + 1,
		// h_amount is randomly selected within [1.00..5000.00]
		h_amount: float64(randInt(100, 500000)) / float64(100.0),
		h_date:   time.Now(),
	}

	// 2.5.1.2: 85% chance of paying through home warehouse, otherwise
	// remote.
	if rand.Intn(100) < 85 {
		d.c_w_id = w_id
		d.c_d_id = d.d_id
	} else {
		d.c_w_id = rand.Intn(*warehouses)
		// Find a c_w_id != w_id if there's more than 1 configured warehouse.
		for d.c_w_id == w_id && *warehouses > 1 {
			d.c_w_id = rand.Intn(*warehouses)
		}
		d.c_d_id = rand.Intn(10) + 1
	}

	// 2.5.1.2: The customer is randomly selected 60% of the time by last name
	// and 40% by number.
	if rand.Intn(9) < 6 {
		d.c_last = randCLast()
	} else {
		d.c_id = randCustomerID()
	}

	if err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		var w_name, d_name string
		// Update warehouse with payment
		if err := tx.QueryRow(`
				UPDATE warehouse
				SET w_ytd = w_ytd + $1
				WHERE w_id = $2
				RETURNING w_name, w_street_1, w_street_2, w_city, w_state, w_zip`,
			d.h_amount, w_id,
		).Scan(&w_name, &d.w_street_1, &d.w_street_2, &d.w_city, &d.w_state, &d.w_zip); err != nil {
			return err
		}

		// Update district with payment
		if err := tx.QueryRow(`
				UPDATE district
				SET d_ytd = d_ytd + $1
				WHERE d_w_id = $2 AND d_id = $3
				RETURNING d_name, d_street_1, d_street_2, d_city, d_state, d_zip`,
			d.h_amount, w_id, d.d_id,
		).Scan(&d_name, &d.d_street_1, &d.d_street_2, &d.d_city, &d.d_state, &d.d_zip); err != nil {
			return err
		}

		// If we are selecting by last name, first find the relevant customer id and
		// then proceed.
		if d.c_id == 0 {
			// 2.5.2.2 Case 2: Pick the middle row, rounded up, from the selection by last name.
			rows, err := tx.Query(`
					SELECT c_id
					FROM customer
					WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3
					ORDER BY c_first ASC`,
				w_id, d.d_id, d.c_last)
			if err != nil {
				return errors.Wrap(err, "select by last name fail")
			}
			customers := make([]int, 0, 1)
			for rows.Next() {
				var c_id int
				err = rows.Scan(&c_id)
				if err != nil {
					rows.Close()
					return err
				}
				customers = append(customers, c_id)
			}
			rows.Close()
			cIdx := len(customers) / 2
			if len(customers)%2 == 0 {
				cIdx -= 1
			}

			d.c_id = customers[cIdx]
		}

		// Update customer with payment.
		if err := tx.QueryRow(`
				UPDATE customer
				SET (c_balance, c_ytd_payment, c_payment_cnt) =
					(c_balance - $1, c_ytd_payment + $1, c_payment_cnt + 1)
				WHERE c_w_id = $2 AND c_d_id = $3 AND c_id = $4
				RETURNING c_first, c_middle, c_last, c_street_1, c_street_2,
						  c_city, c_state, c_zip, c_phone, c_since, c_credit,
						  c_credit_lim, c_discount, c_balance`,
			d.h_amount, d.c_w_id, d.c_d_id, d.c_id,
		).Scan(&d.c_first, &d.c_middle, &d.c_last, &d.c_street_1, &d.c_street_2,
			&d.c_city, &d.c_state, &d.c_zip, &d.c_phone, &d.c_since, &d.c_credit,
			&d.c_credit_lim, &d.c_discount, &d.c_balance,
		); err != nil {
			return errors.Wrap(err, "select by customer idfail")
		}

		if d.c_credit == "BC" {
			// If the customer has bad credit, update the customer's C_DATA.
			d.c_data = fmt.Sprintf("%d %d %d %d %d %d | %s", d.c_id, d.c_d_id, d.c_w_id, d.d_id, w_id, d.h_amount, d.c_data)
			if len(d.c_data) > 500 {
				d.c_data = d.c_data[0:500]
			}
		}
		h_data := fmt.Sprintf("%s    %s", w_name, d_name)

		// Insert history line.
		if _, err := tx.Exec(`
				INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_data)
				VALUES ($1, $2, $3, $4, $5, $6)`,
			d.c_id, d.c_d_id, d.c_w_id, d.d_id, w_id, h_data,
		); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return d, nil
}
