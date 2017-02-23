package main

var query17 = `
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
`
