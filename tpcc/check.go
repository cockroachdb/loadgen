package main

import (
	"database/sql"
	"fmt"

	"time"

	"github.com/pkg/errors"
)

func check3321(db *sql.DB) error {
	// 3.3.2.1 Entries in the WAREHOUSE and DISTRICT tables must satisfy the relationship:
	// W_YTD = sum (D_YTD)

	var rows *sql.Rows
	var err error
	if rows, err = db.Query(`
SELECT w_id, w_ytd, sum_d_ytd 
FROM warehouse 
FULL OUTER JOIN 
(SELECT d_w_id, SUM(d_ytd) as sum_d_ytd FROM district GROUP BY d_w_id) 
ON (w_id = d_w_id)
WHERE w_ytd != sum_d_ytd
`); err != nil {
		return err
	}

	var val float64
	var i int
	for ; rows.Next(); i++ {
		rows.Scan(&val)
	}

	if err := rows.Close(); err != nil {
		return err
	}

	if i != 0 {
		return errors.Errorf("%d rows returned, expected zero", i)
	}

	return nil
}

func check3322(db *sql.DB) error {
	// Entries in the DISTRICT, ORDER, and NEW-ORDER tables must satisfy the relationship:
	// D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)

	districtRows, err := db.Query("SELECT d_next_o_id FROM district ORDER BY d_w_id, d_id")
	if err != nil {
		return err
	}
	newOrderRows, err := db.Query(`
SELECT MAX(no_o_id) FROM tpcc.new_order GROUP BY no_d_id, no_w_id ORDER BY no_w_id, no_d_id
`)
	if err != nil {
		return err
	}
	orderRows, err := db.Query(`
SELECT MAX(o_id) FROM tpcc.order GROUP BY o_d_id, o_w_id ORDER BY o_w_id, o_d_id
`)
	if err != nil {
		return err
	}

	var district, newOrder, order float64
	var i int
	for ; districtRows.Next() && newOrderRows.Next() && orderRows.Next(); i++ {
		districtRows.Scan(&district)
		newOrderRows.Scan(&newOrder)
		orderRows.Scan(&order)

		if (order != newOrder) || (order != (district - 1)) {
			return errors.Errorf("inequality at idx %d: order: %d, newOrder: %d, district-1: %d",
				i, order, newOrder, district-1)
		}
	}
	if districtRows.Next() || newOrderRows.Next() || orderRows.Next() {
		return errors.New("length mismatch between rows")
	}
	if err := districtRows.Close(); err != nil {
		return err
	}
	if err := newOrderRows.Close(); err != nil {
		return err
	}
	if err := orderRows.Close(); err != nil {
		return err
	}

	if i == 0 {
		return errors.Errorf("zero rows")
	}

	return nil
}

func check3323(db *sql.DB) error {
	// max(NO_O_ID) - min(NO_O_ID) + 1 = # of rows in new_order for each warehouse/district
	rows, err := db.Query(`
SELECT MAX(no_o_id) - MIN(no_o_id) - COUNT(*) FROM new_order GROUP BY no_w_id, no_d_id
`)
	if err != nil {
		return err
	}

	var i int
	var val int
	for ; rows.Next(); i++ {
		rows.Scan(&val)
		if val != -1 {
			return errors.Errorf("MAX(no_o_id) - MIN(no_o_id) - COUNT(rows) must be -1, got %d", val)
		}
	}

	if err := rows.Close(); err != nil {
		return err
	}

	if i == 0 {
		return errors.Errorf("zero rows")
	}

	return nil
}

func check3324(db *sql.DB) error {
	// SUM(O_OL_CNT) = [number of rows in the ORDER-LINE table for this district]

	leftRows, err := db.Query(`
SELECT SUM(o_ol_cnt) FROM tpcc.order GROUP BY o_w_id, o_d_id ORDER BY o_w_id, o_d_id
`)
	if err != nil {
		return err
	}
	rightRows, err := db.Query(`
SELECT COUNT(*) FROM tpcc.order_line GROUP BY ol_w_id, ol_d_id ORDER BY ol_w_id, ol_d_id
`)
	if err != nil {
		return err
	}
	var i int
	var left, right int64
	for ; leftRows.Next() && rightRows.Next(); i++ {
		leftRows.Scan(&left)
		rightRows.Scan(&right)
		if left != right {
			return errors.Errorf("order.SUM(o_ol_cnt): %d != order_line.count(*): %d", left, right)
		}
	}
	if i == 0 {
		return errors.Errorf("0 rows returned")
	}
	if leftRows.Next() || rightRows.Next() {
		return errors.Errorf("length of order.SUM(o_ol_cnt) != order_line.count(*)")
	}

	if err := leftRows.Close(); err != nil {
		return err
	}
	if err := rightRows.Close(); err != nil {
		return err
	}

	return nil
}

func check3325(db *sql.DB) error {
	// We want the symmetric difference between the sets:
	// (SELECT no_w_id, no_d_id, no_o_id FROM tpcc.new_order)
	// (SELECT o_w_id, o_d_id, o_id FROM tpcc.order@primary WHERE o_carrier_id IS NULL)
	// We achieve this by two EXCEPT ALL queries.

	firstQuery, err := db.Query(`
(SELECT no_w_id, no_d_id, no_o_id FROM tpcc.new_order) 
EXCEPT ALL 
(SELECT o_w_id, o_d_id, o_id FROM tpcc.order@primary WHERE o_carrier_id IS NULL)`)
	if err != nil {
		return err
	}
	secondQuery, err := db.Query(`
(SELECT o_w_id, o_d_id, o_id FROM tpcc.order@primary WHERE o_carrier_id IS NULL) 
EXCEPT ALL 
(SELECT no_w_id, no_d_id, no_o_id FROM tpcc.new_order)`)
	if err != nil {
		return err
	}

	if firstQuery.Next() {
		return errors.Errorf("left EXCEPT right returned nonzero results.")
	}

	if secondQuery.Next() {
		return errors.Errorf("right EXCEPT left returned nonzero results.")
	}

	if err := firstQuery.Close(); err != nil {
		return err
	}
	if err := secondQuery.Close(); err != nil {
		return err
	}

	return nil
}

func check3326(db *sql.DB) error {
	firstQuery, err := db.Query(`
(SELECT o_w_id, o_d_id, o_id, o_ol_cnt FROM tpcc.order 
  ORDER BY o_w_id, o_d_id, o_id DESC) 
EXCEPT ALL 
(SELECT ol_w_id, ol_d_id, ol_o_id, COUNT(*) FROM tpcc.order_line 
  GROUP BY (ol_w_id, ol_d_id, ol_o_id) 
  ORDER BY ol_w_id, ol_d_id, ol_o_id DESC)`)
	if err != nil {
		return err
	}
	secondQuery, err := db.Query(`
(SELECT ol_w_id, ol_d_id, ol_o_id, COUNT(*) FROM tpcc.order_line 
  GROUP BY (ol_w_id, ol_d_id, ol_o_id) ORDER BY ol_w_id, ol_d_id, ol_o_id DESC) 
EXCEPT ALL 
(SELECT o_w_id, o_d_id, o_id, o_ol_cnt FROM tpcc.order 
  ORDER BY o_w_id, o_d_id, o_id DESC)`)
	if err != nil {
		return err
	}

	if firstQuery.Next() {
		return errors.Errorf("left EXCEPT right returned nonzero results")
	}

	if secondQuery.Next() {
		return errors.Errorf("right EXCEPT left returned nonzero results")
	}

	if err := firstQuery.Close(); err != nil {
		return err
	}
	if err := secondQuery.Close(); err != nil {
		return err
	}

	return nil
}

func checkConsistency(db *sql.DB) bool {
	type check struct {
		name      string
		f         func(db *sql.DB) error
		expensive bool
	}

	checks := []check{
		{"3.3.2.1", check3321, false},
		{"3.3.2.2", check3322, false},
		{"3.3.2.3", check3323, false},
		{"3.3.2.4", check3324, false},
		{"3.3.2.5", check3325, false},
		{"3.3.2.6", check3326, true},
	}
	var errorEncountered bool

	// TODO(arjun): We should run each test in a single transaction as currently
	// we have to shut down load before running the checks.

	for _, check := range checks {
		if check.expensive && !*expensive {
			continue
		}
		start := time.Now()
		fmt.Printf(check.name)
		if err := check.f(db); err != nil {
			fmt.Printf(": error encountered: '%+v'", err)
			errorEncountered = true
		} else {
			fmt.Printf(" passed")
		}

		fmt.Printf(" after %f seconds.\n", time.Since(start).Seconds())
	}
	return errorEncountered
}
