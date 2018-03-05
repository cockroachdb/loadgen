package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"math"
)

func partitionWarehouse(db *sql.DB, wIDs []int) {
	var buf bytes.Buffer
	buf.WriteString("ALTER TABLE warehouse PARTITION BY RANGE (w_id) (\n")
	for i, n := 0, len(wIDs)-1; i < n; i++ {
		fmt.Fprintf(&buf, "  PARTITION p%d VALUES FROM (%d) to (%d)", i, wIDs[i], wIDs[i+1])
		if i+1 < n {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(")\n")
	if _, err := db.Exec(buf.String()); err != nil {
		panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
	}
}

func partitionDistrict(db *sql.DB, wIDs []int) {
	var buf bytes.Buffer
	buf.WriteString("ALTER TABLE district PARTITION BY RANGE (d_w_id) (\n")
	for i, n := 0, len(wIDs)-1; i < n; i++ {
		fmt.Fprintf(&buf, "  PARTITION p%d VALUES FROM (%d) to (%d)", i, wIDs[i], wIDs[i+1])
		if i+1 < n {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(")\n")
	if _, err := db.Exec(buf.String()); err != nil {
		panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
	}
}

func partitionNewOrder(db *sql.DB, wIDs []int) {
	var buf bytes.Buffer
	buf.WriteString("ALTER TABLE new_order PARTITION BY RANGE (no_w_id) (\n")
	for i, n := 0, len(wIDs)-1; i < n; i++ {
		fmt.Fprintf(&buf, "  PARTITION p%d VALUES FROM (%d) to (%d)", i, wIDs[i], wIDs[i+1])
		if i+1 < n {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(")\n")
	if _, err := db.Exec(buf.String()); err != nil {
		panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
	}
}

func partitionOrder(db *sql.DB, wIDs []int) {
	targets := []string{
		`TABLE "order"`,
		`INDEX "order"@order_idx`,
		`INDEX "order"@order_o_w_id_o_d_id_o_c_id_idx`,
	}

	for j, target := range targets {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "ALTER %s PARTITION BY RANGE (o_w_id) (\n", target)
		for i, n := 0, len(wIDs)-1; i < n; i++ {
			fmt.Fprintf(&buf, "  PARTITION p%d_%d VALUES FROM (%d) to (%d)", j, i, wIDs[i], wIDs[i+1])
			if i+1 < n {
				buf.WriteString(",")
			}
			buf.WriteString("\n")
		}
		buf.WriteString(")\n")
		if _, err := db.Exec(buf.String()); err != nil {
			panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
		}
	}
}

func partitionOrderLine(db *sql.DB, wIDs []int) {
	data := []struct {
		target string
		column string
	}{
		{`TABLE order_line`, `ol_w_id`},
		{`INDEX order_line@order_line_fk`, `ol_supply_w_id`},
	}

	for j, d := range data {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "ALTER %s PARTITION BY RANGE (%s) (\n", d.target, d.column)
		for i, n := 0, len(wIDs)-1; i < n; i++ {
			fmt.Fprintf(&buf, "  PARTITION p%d_%d VALUES FROM (%d) to (%d)", j, i, wIDs[i], wIDs[i+1])
			if i+1 < n {
				buf.WriteString(",")
			}
			buf.WriteString("\n")
		}
		buf.WriteString(")\n")
		if _, err := db.Exec(buf.String()); err != nil {
			panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
		}
	}
}

func partitionStock(db *sql.DB, wIDs []int) {
	var buf bytes.Buffer
	buf.WriteString("ALTER TABLE stock PARTITION BY RANGE (s_w_id) (\n")
	for i, n := 0, len(wIDs)-1; i < n; i++ {
		fmt.Fprintf(&buf, "  PARTITION p%d VALUES FROM (%d) to (%d)", i, wIDs[i], wIDs[i+1])
		if i+1 < n {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(")\n")
	if _, err := db.Exec(buf.String()); err != nil {
		panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
	}

	// TODO(peter): partition stock@stock_s_i_id_idx
}

func partitionCustomer(db *sql.DB, wIDs []int) {
	targets := []string{
		`TABLE customer`,
		`INDEX customer@customer_idx`,
	}

	for j, target := range targets {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "ALTER %s PARTITION BY RANGE (c_w_id) (\n", target)
		for i, n := 0, len(wIDs)-1; i < n; i++ {
			fmt.Fprintf(&buf, "  PARTITION p%d_%d VALUES FROM (%d) to (%d)", j, i, wIDs[i], wIDs[i+1])
			if i+1 < n {
				buf.WriteString(",")
			}
			buf.WriteString("\n")
		}
		buf.WriteString(")\n")
		if _, err := db.Exec(buf.String()); err != nil {
			panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
		}
	}
}

func partitionHistory(db *sql.DB, wIDs []int, partitions int) {
	// NB: We use rand.Int63() to populate the history.rowid column. That
	// function returns a non-negative 63-bit integer.
	const maxVal = math.MaxInt64
	rowids := make([]int, partitions+1)
	for i := 0; i < partitions; i++ {
		rowids[i] = i * (maxVal / partitions)
	}
	rowids[partitions] = maxVal

	var buf bytes.Buffer
	buf.WriteString("ALTER TABLE history PARTITION BY RANGE (rowid) (\n")
	for i, n := 0, len(rowids)-1; i < n; i++ {
		fmt.Fprintf(&buf, "  PARTITION p%d VALUES FROM (%d) to (%d)", i, rowids[i], rowids[i+1])
		if i+1 < n {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(")\n")
	if _, err := db.Exec(buf.String()); err != nil {
		panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
	}

	data := []struct {
		target string
		column string
	}{
		{`INDEX history@history_h_w_id_h_d_id_idx`, `h_w_id`},
		{`INDEX history@history_h_c_w_id_h_c_d_id_h_c_id_idx`, `h_c_w_id`},
	}

	for j, d := range data {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "ALTER %s PARTITION BY RANGE (%s) (\n", d.target, d.column)
		for i, n := 0, len(wIDs)-1; i < n; i++ {
			fmt.Fprintf(&buf, "  PARTITION p%d_%d VALUES FROM (%d) to (%d)", j, i, wIDs[i], wIDs[i+1])
			if i+1 < n {
				buf.WriteString(",")
			}
			buf.WriteString("\n")
		}
		buf.WriteString(")\n")
		if _, err := db.Exec(buf.String()); err != nil {
			panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
		}
	}
}

func partitionItem(db *sql.DB, partitions int) {
	const nItems = 100000
	iIDs := make([]int, partitions+1)
	for i := 0; i < partitions; i++ {
		iIDs[i] = i * (nItems / partitions)
	}
	iIDs[partitions] = nItems

	var buf bytes.Buffer
	buf.WriteString("ALTER TABLE item PARTITION BY RANGE (i_id) (\n")
	for i, n := 0, len(iIDs)-1; i < n; i++ {
		fmt.Fprintf(&buf, "  PARTITION p%d VALUES FROM (%d) to (%d)", i, iIDs[i], iIDs[i+1])
		if i+1 < n {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(")\n")
	if _, err := db.Exec(buf.String()); err != nil {
		panic(fmt.Sprintf("Couldn't exec %s: %s\n", buf.String(), err))
	}
}

func partitionTables(db *sql.DB, warehouses, partitions int) {
	wIDs := make([]int, partitions+1)
	for i := 0; i < partitions; i++ {
		wIDs[i] = i * (warehouses / partitions)
	}
	wIDs[partitions] = warehouses

	partitionWarehouse(db, wIDs)
	partitionDistrict(db, wIDs)
	partitionNewOrder(db, wIDs)
	partitionOrder(db, wIDs)
	partitionOrderLine(db, wIDs)
	partitionStock(db, wIDs)
	partitionCustomer(db, wIDs)
	partitionHistory(db, wIDs, partitions)
	partitionItem(db, partitions)
}
