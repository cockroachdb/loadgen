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
	1: `
SELECT
	l_returnflag,
	l_linestatus,
	SUM(l_quantity) AS sum_qty,
	SUM(l_extendedprice) AS sum_base_price,
	SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
	SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
	AVG(l_quantity) AS avg_qty,
	AVG(l_extendedprice) AS avg_price,
	AVG(l_discount) AS avg_disc,
	COUNT(*) AS count_order
FROM
	lineitem
WHERE
	l_shipdate <= DATE '1998-12-01' - INTERVAL '95' DAY
GROUP BY
	l_returnflag,
	l_linestatus
ORDER BY
	l_returnflag,
	l_linestatus;
`,
	2: `
SELECT
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
FROM
	part,
	supplier,
	partsupp,
	nation,
	region
WHERE
	p_partkey = ps_partkey
	AND s_suppkey = ps_suppkey
	AND p_size = 42
	AND p_type LIKE '%STEEL'
	AND s_nationkey = n_nationkey
	AND n_regionkey = r_regionkey
	AND r_name = 'AMERICA'
	AND ps_supplycost = (
		SELECT
			min(ps_supplycost)
		FROM
			partsupp,
			supplier,
			nation,
			region
		WHERE
			p_partkey = ps_partkey
			AND s_suppkey = ps_suppkey
			AND s_nationkey = n_nationkey
			AND n_regionkey = r_regionkey
			AND r_name = 'AMERICA'
	)
ORDER BY
	s_acctbal DESC,
	n_name,
	s_name,
	p_partkey;
`,
	3: `
SELECT
	l_orderkey,
	SUM(l_extendedprice * (1 - l_discount)) AS revenue,
	o_orderdate,
	o_shippriority
FROM
	customer,
	orders,
	lineitem
WHERE
	c_mktsegment = 'MACHINERY'
	AND c_custkey = o_custkey
	AND l_orderkey = o_orderkey
	AND o_orderDATE < DATE '1995-03-10'
	AND l_shipdate > DATE '1995-03-10'
GROUP BY
	l_orderkey,
	o_orderdate,
	o_shippriority
ORDER BY
	revenue DESC,
	o_orderdate;
`,
	4: `
SELECT
	o_orderpriority,
	COUNT(*) AS order_count
FROM
	orders
WHERE
	o_orderdate >= DATE '1994-08-01'
	AND o_orderdate < DATE '1994-08-01' + INTERVAL '3' MONTH
	AND EXISTS (
		SELECT
			*
		FROM
			lineitem
		WHERE
			l_orderkey = o_orderkey
			AND l_commitDATE < l_receiptdate
	)
GROUP BY
	o_orderpriority
ORDER BY
	o_orderpriority;
`,
	5: `
SELECT
	n_name,
	SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
WHERE
	c_custkey = o_custkey
	AND l_orderkey = o_orderkey
	AND l_suppkey = s_suppkey
	AND c_nationkey = s_nationkey
	AND s_nationkey = n_nationkey
	AND n_regionkey = r_regionkey
	AND r_name = 'AFRICA'
	AND o_orderDATE >= DATE '1997-01-01'
	AND o_orderDATE < DATE '1997-01-01' + INTERVAL '1' YEAR
GROUP BY
	n_name
ORDER BY
	revenue DESC;
`,
	6: `
SELECT
	SUM(l_extendedprice * l_discount) AS revenue
FROM
	lineitem
WHERE
	l_shipdate >= DATE '1997-01-01'
	AND l_shipdate < DATE '1997-01-01' + INTERVAL '1' YEAR
	AND l_discount BETWEEN 0.07 - 0.01 AND 0.07 + 0.01
	AND l_quantity < 24;
`,
	7: `
SELECT
	supp_nation,
	cust_nation,
	l_year,
	SUM(volume) AS revenue
FROM
	(
		SELECT
			n1.n_name AS supp_nation,
			n2.n_name AS cust_nation,
			EXTRACT(year FROM l_shipdate) AS l_year,
			l_extendedprice * (1 - l_discount) AS volume
		FROM
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2
		WHERE
			s_suppkey = l_suppkey
			AND o_orderkey = l_orderkey
			AND c_custkey = o_custkey
			AND s_nationkey = n1.n_nationkey
			AND c_nationkey = n2.n_nationkey
			AND (
				(n1.n_name = 'MOZAMBIQUE' AND n2.n_name = 'CANADA')
				or (n1.n_name = 'CANADA' AND n2.n_name = 'MOZAMBIQUE')
			)
			AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
	) AS shipping
GROUP BY
	supp_nation,
	cust_nation,
	l_year
ORDER BY
	supp_nation,
	cust_nation,
	l_year;
`,
	8: `
SELECT
	o_year,
	SUM(CASE
		WHEN nation = 'CANADA' THEN volume
		ELSE 0
	END) / SUM(volume) AS mkt_share
FROM
	(
		SELECT
			EXTRACT(year FROM o_orderdate) AS o_year,
			l_extendedprice * (1 - l_discount) AS volume,
			n2.n_name AS nation
		FROM
			part,
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2,
			region
		WHERE
			p_partkey = l_partkey
			AND s_suppkey = l_suppkey
			AND l_orderkey = o_orderkey
			AND o_custkey = c_custkey
			AND c_nationkey = n1.n_nationkey
			AND n1.n_regionkey = r_regionkey
			AND r_name = 'AMERICA'
			AND s_nationkey = n2.n_nationkey
			AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
			AND p_type = 'ECONOMY POLISHED STEEL'
	) AS all_nations
GROUP BY
	o_year
ORDER BY
	o_year;
`,
	9: `
SELECT
	nation,
	o_year,
	SUM(amount) AS sum_profit
FROM
	(
		SELECT
			n_name AS nation,
			EXTRACT(year FROM o_orderdate) AS o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
		FROM
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		WHERE
			s_suppkey = l_suppkey
			AND ps_suppkey = l_suppkey
			AND ps_partkey = l_partkey
			AND p_partkey = l_partkey
			AND o_orderkey = l_orderkey
			AND s_nationkey = n_nationkey
			AND p_name LIKE '%royal%'
	) AS profit
GROUP BY
	nation,
	o_year
ORDER BY
	nation,
	o_year DESC;
`,
	10: `
SELECT
	c_custkey,
	c_name,
	SUM(l_extendedprice * (1 - l_discount)) AS revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
FROM
	customer,
	orders,
	lineitem,
	nation
WHERE
	c_custkey = o_custkey
	AND l_orderkey = o_orderkey
	AND o_orderDATE >= DATE '1994-12-01'
	AND o_orderDATE < DATE '1994-12-01' + INTERVAL '3' MONTH
	AND l_returnflag = 'R'
	AND c_nationkey = n_nationkey
GROUP BY
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
ORDER BY
	revenue DESC;
`,
	11: `
SELECT
	ps_partkey,
	SUM(ps_supplycost * ps_availqty) AS value
FROM
	partsupp,
	supplier,
	nation
WHERE
	ps_suppkey = s_suppkey
	AND s_nationkey = n_nationkey
	AND n_name = 'ETHIOPIA'
GROUP BY
	ps_partkey HAVING
		SUM(ps_supplycost * ps_availqty) > (
			SELECT
				SUM(ps_supplycost * ps_availqty) * 0.0000003333
			FROM
				partsupp,
				supplier,
				nation
			WHERE
				ps_suppkey = s_suppkey
				AND s_nationkey = n_nationkey
				AND n_name = 'ETHIOPIA'
		)
ORDER BY
	value DESC;
`,
	12: `
SELECT
	l_shipmode,
	SUM(CASE
		WHEN o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			THEN 1
		ELSE 0
	END) AS high_line_count,
	SUM(CASE
		WHEN o_orderpriority <> '1-URGENT'
			AND o_orderpriority <> '2-HIGH'
			THEN 1
		ELSE 0
	END) AS low_line_count
FROM
	orders,
	lineitem
WHERE
	o_orderkey = l_orderkey
	AND l_shipmode IN ('AIR', 'REG AIR')
	AND l_commitdate < l_receiptdate
	AND l_shipdate < l_commitdate
	AND l_receiptdate >= DATE '1997-01-01'
	AND l_receiptdate < DATE '1997-01-01' + INTERVAL '1' YEAR
GROUP BY
	l_shipmode
ORDER BY
	l_shipmode;
`,
	13: `
SELECT
	c_count,
	COUNT(*) AS custdist
FROM
	(
		SELECT
			c_custkey,
			COUNT(o_orderkey) AS c_count
		FROM
			customer LEFT OUTER JOIN orders ON
				c_custkey = o_custkey
				AND o_comment NOT LIKE '%special%deposits%'
		GROUP BY
			c_custkey
	) AS c_orders
GROUP BY
	c_count
ORDER BY
	custdist DESC,
	c_count DESC;
`,
	14: `
SELECT
	100.00 * SUM(CASE
		WHEN p_type LIKE 'PROMO%'
			THEN l_extendedprice * (1 - l_discount)
		ELSE 0
	END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
	lineitem,
	part
WHERE
	l_partkey = p_partkey
	AND l_shipdate >= DATE '1997-04-01'
	AND l_shipdate < DATE '1997-04-01' + INTERVAL '1' MONTH;
`,
	15: `
CREATE VIEW revenue0 (supplier_no, total_revenue) AS
	SELECT
		l_suppkey,
		SUM(l_extendedprice * (1 - l_discount))
	FROM
		lineitem
	WHERE
		l_shipdate >= DATE '1997-03-01'
		AND l_shipdate < DATE '1997-03-01' + INTERVAL '3' MONTH
	GROUP BY
		l_suppkey;

SELECT
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
FROM
	supplier,
	revenue0
WHERE
	s_suppkey = supplier_no
	AND total_revenue = (
		SELECT
			MAX(total_revenue)
		FROM
			revenue0
	)
ORDER BY
	s_suppkey;

DROP VIEW revenue0;
`,
	16: `
SELECT
	p_brand,
	p_type,
	p_size,
	COUNT(distinct ps_suppkey) AS supplier_cnt
FROM
	partsupp,
	part
WHERE
	p_partkey = ps_partkey
	AND p_brand <> 'Brand#41'
	AND p_type NOT LIKE 'ECONOMY BURNISHED%'
	AND p_size IN (22, 33, 42, 5, 27, 49, 4, 18)
	AND ps_suppkey NOT IN (
		SELECT
			s_suppkey
		FROM
			supplier
		WHERE
			s_comment LIKE '%Customer%Complaints%'
	)
GROUP BY
	p_brand,
	p_type,
	p_size
ORDER BY
	supplier_cnt DESC,
	p_brand,
	p_type,
	p_size;
`,
	17: `
SELECT
	SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM
	lineitem,
	part
WHERE
	p_partkey = l_partkey
	AND p_brand = 'Brand#14'
	AND p_container = 'MED BOX'
	AND l_quantity < (
		SELECT
			0.2 * AVG(l_quantity)
		FROM
			lineitem
		WHERE
			l_partkey = p_partkey
	);
`,
	18: `
SELECT
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	SUM(l_quantity)
FROM
	customer,
	orders,
	lineitem
WHERE
	o_orderkey IN (
		SELECT
			l_orderkey
		FROM
			lineitem
		GROUP BY
			l_orderkey HAVING
				SUM(l_quantity) > 314
	)
	AND c_custkey = o_custkey
	AND o_orderkey = l_orderkey
GROUP BY
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
ORDER BY
	o_totalprice DESC,
	o_orderdate;
`,
	19: `
SELECT
	SUM(l_extendedprice* (1 - l_discount)) AS revenue
FROM
	lineitem,
	part
WHERE
	(
		p_partkey = l_partkey
		AND p_brand = 'Brand#34'
		AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		AND l_quantity >= 5 AND l_quantity <= 5 + 10
		AND p_size BETWEEN 1 AND 5
		AND l_shipmode IN ('AIR', 'AIR REG')
		AND l_shipinstruct = 'DELIVER IN PERSON'
	)
	OR
	(
		p_partkey = l_partkey
		AND p_brand = 'Brand#51'
		AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		AND l_quantity >= 12 AND l_quantity <= 12 + 10
		AND p_size BETWEEN 1 AND 10
		AND l_shipmode IN ('AIR', 'AIR REG')
		AND l_shipinstruct = 'DELIVER IN PERSON'
	)
	OR
	(
		p_partkey = l_partkey
		AND p_brand = 'Brand#35'
		AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		AND l_quantity >= 30 AND l_quantity <= 30 + 10
		AND p_size BETWEEN 1 AND 15
		AND l_shipmode IN ('AIR', 'AIR REG')
		AND l_shipinstruct = 'DELIVER IN PERSON'
	);
`,
	20: `
SELECT
	s_name,
	s_address
FROM
	supplier,
	nation
WHERE
	s_suppkey IN (
		SELECT
			ps_suppkey
		FROM
			partsupp
		WHERE
			ps_partkey IN (
				SELECT
					p_partkey
				FROM
					part
				WHERE
					p_name LIKE 'orange%'
			)
			AND ps_availqty > (
				SELECT
					0.5 * SUM(l_quantity)
				FROM
					lineitem
				WHERE
					l_partkey = ps_partkey
					AND l_suppkey = ps_suppkey
					AND l_shipdate >= DATE '1997-01-01'
					AND l_shipdate < DATE '1997-01-01' + INTERVAL '1' YEAR
			)
	)
	AND s_nationkey = n_nationkey
	AND n_name = 'ALGERIA'
ORDER BY
	s_name;
`,
	21: `
SELECT
	s_name,
	COUNT(*) AS numwait
FROM
	supplier,
	lineitem l1,
	orders,
	nation
WHERE
	s_suppkey = l1.l_suppkey
	AND o_orderkey = l1.l_orderkey
	AND o_orderstatus = 'F'
	AND l1.l_receiptDATE > l1.l_commitdate
	AND EXISTS (
		SELECT
			*
		FROM
			lineitem l2
		WHERE
			l2.l_orderkey = l1.l_orderkey
			AND l2.l_suppkey <> l1.l_suppkey
	)
	AND NOT EXISTS (
		SELECT
			*
		FROM
			lineitem l3
		WHERE
			l3.l_orderkey = l1.l_orderkey
			AND l3.l_suppkey <> l1.l_suppkey
			AND l3.l_receiptDATE > l3.l_commitdate
	)
	AND s_nationkey = n_nationkey
	AND n_name = 'SAUDI ARABIA'
GROUP BY
	s_name
ORDER BY
	numwait DESC,
	s_name;
`,
	22: `
SELECT
	cntrycode,
	COUNT(*) AS numcust,
	SUM(c_acctbal) AS totacctbal
FROM
	(
		SELECT
			substring(c_phone FROM 1 FOR 2) AS cntrycode,
			c_acctbal
		FROM
			customer
		WHERE
			substring(c_phone FROM 1 FOR 2) in
				('20', '32', '44', '33', '29', '22', '31')
			AND c_acctbal > (
				SELECT
					AVG(c_acctbal)
				FROM
					customer
				WHERE
					c_acctbal > 0.00
					AND substring(c_phone FROM 1 FOR 2) in
						('20', '32', '44', '33', '29', '22', '31')
			)
			AND NOT EXISTS (
				SELECT
					*
				FROM
					orders
				WHERE
					o_custkey = c_custkey
			)
	) AS custsale
GROUP BY
	cntrycode
ORDER BY
	cntrycode;
`,
}

func runQuery(db *sql.DB, query int) (int, error) {
	var queryString string
	if *distsql {
		queryString = "SET dist_sql = 'always'; "
	}
	queryString = fmt.Sprintf("%s%s", queryString, queryStmts[query])

	switch query {
	case 2, 4, 13, 16, 17, 18, 20, 21, 22:
		fmt.Println("Warning: query is unsupported")
	case 5, 6, 10, 12, 14:
		fmt.Println("Warning: query causes Cockroach to panic (see #13692)")
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
