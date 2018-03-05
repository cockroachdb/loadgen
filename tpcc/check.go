package main

import (
	"database/sql"
	"fmt"

	"time"

	"github.com/pkg/errors"
)

func check3321(db *sql.DB) error {
	start := time.Now()

	var rowsLeft, rowsRight *sql.Rows
	var err error
	if rowsLeft, err = db.Query("SELECT w_ytd FROM warehouse ORDER BY w_id"); err != nil {
		return err
	}

	if rowsRight, err = db.Query("SELECT SUM(d_ytd) FROM district GROUP BY d_w_id ORDER BY d_w_id"); err != nil {
		return err
	}

	var left, right float64
	var i int
	for ; rowsLeft.Next() && rowsRight.Next(); i++ {
		rowsLeft.Scan(&left)
		rowsRight.Scan(&right)

		if left != right {
			return errors.Errorf("warehouse %d: warehouse.w_ytd %f != sum(district.d_ytd) %f",
				i, left, right)
		}
	}

	if rowsLeft.Next() || rowsRight.Next() {
		return errors.Errorf("length mismatch between warehouse.w_ytd and sum(district.d_ytd)")
	}
	if err := rowsLeft.Close(); err != nil {
		return err
	}
	if err := rowsRight.Close(); err != nil {
		return err
	}

	if i == 0 {
		return errors.Errorf("zero rows")
	}

	fmt.Printf("3.3.2.1: %d rows, passed in %f seconds.\n", i, time.Since(start).Seconds())
	return nil
}

func check3322(db *sql.DB) error {
	start := time.Now()

	districtRows, err := db.Query("SELECT d_next_o_id FROM district ORDER BY d_w_id, d_id")
	if err != nil {
		return err
	}
	newOrderRows, err := db.Query("SELECT MAX(no_o_id) FROM tpcc.new_order GROUP BY no_d_id, no_w_id ORDER BY no_w_id, no_d_id")
	if err != nil {
		return err
	}
	orderRows, err := db.Query("SELECT MAX(o_id) FROM tpcc.order GROUP BY o_d_id, o_w_id ORDER BY o_w_id, o_d_id")
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

	fmt.Printf("3.3.2.2: %d rows, passed in %f seconds.\n", i, time.Since(start).Seconds())
	return nil
}

func check3323(db *sql.DB) error {
	start := time.Now()

	// max(NO_O_ID) - min(NO_O_ID) + 1 = # of rows in new_order for each warehouse/district
	rows, err := db.Query(`SELECT MAX(no_o_id) - MIN(no_o_id) - COUNT(*) FROM new_order GROUP BY no_w_id, no_d_id`)
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

	fmt.Printf("3.3.2.3: %d rows, passed in %f seconds.\n", i, time.Since(start).Seconds())
	return nil
}

func check3324(db *sql.DB) error {
	start := time.Now()

	leftRows, err := db.Query("SELECT SUM(o_ol_cnt) FROM tpcc.order GROUP BY o_w_id, o_d_id ORDER BY o_w_id, o_d_id")
	if err != nil {
		return err
	}
	rightRows, err := db.Query("SELECT count(*) from tpcc.order_line GROUP BY ol_w_id, ol_d_id ORDER BY ol_w_id, ol_d_id")
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

	fmt.Printf("3.3.2.4: %d rows, passed in %f seconds.\n", i, time.Since(start).Seconds())
	return nil
}

func check3325(db *sql.DB) error {
	start := time.Now()

	// We want the symmetric difference between the sets:
	// (SELECT no_w_id, no_d_id, no_o_id FROM tpcc.new_order)
	// (SELECT o_w_id, o_d_id, o_id FROM tpcc.order@primary WHERE o_carrier_id IS NULL)
	// We achieve this by two EXCEPT ALL queries.

	firstQuery, err := db.Query("(SELECT no_w_id, no_d_id, no_o_id FROM tpcc.new_order) EXCEPT ALL (SELECT o_w_id, o_d_id, o_id FROM tpcc.order@primary WHERE o_carrier_id IS NULL)")
	if err != nil {
		return err
	}
	secondQuery, err := db.Query("(SELECT o_w_id, o_d_id, o_id FROM tpcc.order@primary WHERE o_carrier_id IS NULL) EXCEPT ALL (SELECT no_w_id, no_d_id, no_o_id FROM tpcc.new_order)")
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

	fmt.Printf("3.3.2.5: passed in %f seconds.\n", time.Since(start).Seconds())
	return nil
}

func check3326(db *sql.DB) error {
	// return errors.Errorf("Do not call this check, as its prohibitvely expensive until CockroachDB has a streaming GROUP BY aggregation.")
	start := time.Now()

	// We want the symmetric difference between the sets:
	// (SELECT o_w_id, o_d_id, o_id, o_ol_cnt FROM tpcc.order ORDER BY o_w_id, o_d_id, o_id DESC)
	// (SELECT ol_w_id, ol_d_id, ol_o_id, COUNT(*) FROM tpcc.order_line GROUP BY (ol_w_id, ol_d_id, ol_o_id) ORDER BY ol_w_id, ol_d_id, ol_o_id DESC)
	// We achieve this by two EXCEPT ALL queries.

	firstQuery, err := db.Query("(SELECT o_w_id, o_d_id, o_id, o_ol_cnt FROM tpcc.order ORDER BY o_w_id, o_d_id, o_id DESC) EXCEPT ALL (SELECT ol_w_id, ol_d_id, ol_o_id, COUNT(*) FROM tpcc.order_line GROUP BY (ol_w_id, ol_d_id, ol_o_id) ORDER BY ol_w_id, ol_d_id, ol_o_id DESC)")
	if err != nil {
		return err
	}
	secondQuery, err := db.Query("(SELECT ol_w_id, ol_d_id, ol_o_id, COUNT(*) FROM tpcc.order_line GROUP BY (ol_w_id, ol_d_id, ol_o_id) ORDER BY ol_w_id, ol_d_id, ol_o_id DESC) EXCEPT ALL (SELECT o_w_id, o_d_id, o_id, o_ol_cnt FROM tpcc.order ORDER BY o_w_id, o_d_id, o_id DESC)")
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

	fmt.Printf("3.3.2.6: passed in %f seconds.\n", time.Since(start).Seconds())
	return nil
}

func checkConsistency(db *sql.DB) error {
	type checkFunc func(db *sql.DB) error

	// TODO(arjun): It is important that we run each tests in a
	// single transaction as otherwise we cannot run these checks
	// on a live running TPC-C database.
	db.SetMaxOpenConns(10)

	checks := []checkFunc{
		check3321, check3322, check3323, check3324, check3325,
	}
	expensiveChecks := []checkFunc{
		check3326,
	}

	for _, check := range checks {
		if err := check(db); err != nil {
			return err
		}
	}

	for _, check := range expensiveChecks {
		if err := check(db); err != nil {
			return err
		}
	}

	return nil
}
