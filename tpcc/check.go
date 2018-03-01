package main

import (
	"database/sql"
	"fmt"
)

func forEachWarehouseAndDistrict(fn func(wID, dID int) error) error {
	for wID := 0; wID < *warehouses; wID++ {
		for dID := 1; dID < 10; dID++ {
			if err := fn(wID, dID); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkConsistency(db *sql.DB) error {
	// Reused checks

	maxNewOIDs := make([]int, *warehouses*10)

	sliceIdx := func(wID, dID int) int {
		return wID*10 + dID - 1
	}

	// 3.3.2.1
	// W_YTD = sum(D_YTD) for all warehouses and districts
	var wYTD, sumDYTD float64
	for i := 0; i < *warehouses; i++ {
		if err := db.QueryRow("SELECT w_ytd FROM warehouse WHERE w_id=$1", i).Scan(&wYTD); err != nil {
			return err
		}
		if err := db.QueryRow("SELECT SUM(d_ytd) FROM district WHERE d_w_id=$1", i).Scan(&sumDYTD); err != nil {
			return err
		}
		if wYTD != sumDYTD {
			fmt.Printf("check failed: w_ytd=%f != sum(d_ytd)=%f for warehouse %d\n", wYTD, sumDYTD, i)
		}
	}

	// 3.3.2.2
	// D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID) for all districts
	var dNextOID, maxOID, maxNewOID int
	if err := forEachWarehouseAndDistrict(func(wID, dID int) error {
		if err := db.QueryRow(`SELECT d_next_o_id FROM district WHERE d_w_id=$1 AND d_id=$2`,
			wID, dID,
		).Scan(&dNextOID); err != nil {
			return err
		}
		if err := db.QueryRow(`SELECT max(o_id) FROM "order" WHERE o_w_id=$1 AND o_d_id=$2`,
			wID, dID,
		).Scan(&maxOID); err != nil {
			return err
		}
		if err := db.QueryRow(`SELECT max(no_o_id) FROM new_order WHERE no_w_id=$1 AND no_d_id=$2`,
			wID, dID,
		).Scan(&maxNewOID); err != nil {
			return err
		}
		maxNewOIDs[sliceIdx(wID, dID)] = maxNewOID

		if dNextOID != maxOID+1 || dNextOID != maxNewOID+1 {
			fmt.Printf("check failed: d_next_o_id=%d != max(o_id)+1=%d != max(no_o_id)+1=%d for warehouse %d district %d\n",
				dNextOID, maxOID, maxNewOID, wID, dID)
		}
		return nil
	}); err != nil {
		return err
	}

	// 3.3.2.3
	// max(NO_O_ID) - min(NO_O_ID) + 1 = # of rows in new_order for each warehouse/district
	var minNewOID, countNewOrder int
	if err := forEachWarehouseAndDistrict(func(wID, dID int) error {
		if err := db.QueryRow(`SELECT min(no_o_id), count(*) FROM new_order WHERE no_w_id=$1 AND no_d_id=$2`,
			wID, dID,
		).Scan(&minNewOID, &countNewOrder); err != nil {
			return err
		}
		maxNewOID := maxNewOIDs[sliceIdx(wID, dID)]
		if maxNewOID-minNewOID+1 != countNewOrder {
			fmt.Printf("check failed: max(no_o_id)=%d + min(no_o_id)=%d + 1 != count(*)=%d for w_id=%d d_id=%d\n",
				maxNewOID, minNewOID, countNewOrder, wID, dID)
		}
		return nil
	}); err != nil {
		return err
	}

	// 3.3.2.4
	// sum(O_OL_CNT) = # of rows in the ORDER-LINE table for each warehouse/district
	var sumOOLCnt, countNewOrderLine int
	if err := forEachWarehouseAndDistrict(func(wID, dID int) error {
		if err := db.QueryRow(`SELECT sum(o_ol_cnt) FROM "order" WHERE o_w_id=$1 AND o_d_id=$2`,
			wID, dID,
		).Scan(&sumOOLCnt); err != nil {
			return err
		}
		if err := db.QueryRow(`SELECT count(*) FROM order_line WHERE ol_w_id=$1 AND ol_d_id=$2`,
			wID, dID,
		).Scan(&countNewOrderLine); err != nil {
			return err
		}
		if countNewOrderLine != sumOOLCnt {
			fmt.Printf("check failed: count(order_line)=%d != sum(o_ol_cnt)=%d for w_id=%d d_id=%d\n",
				countNewOrderLine, sumOOLCnt, wID, dID)
		}
		return nil
	}); err != nil {
		return err
	}

	// 3.3.2.8
	// W_YTD = sum(H_AMOUNT) for each warehouse
	var sumHAmount float64
	for i := 0; i < *warehouses; i++ {
		if err := db.QueryRow("SELECT w_ytd FROM warehouse WHERE w_id=$1", i).Scan(&wYTD); err != nil {
			return err
		}
		if err := db.QueryRow("SELECT SUM(h_amount) FROM history WHERE h_w_id=$1", i).Scan(&sumHAmount); err != nil {
			return err
		}
		if wYTD != sumHAmount {
			fmt.Printf("check failed: w_ytd=%f != sum(h_amount)=%f for warehouse %d\n", wYTD, sumHAmount, i)
		}
	}

	return nil
}
