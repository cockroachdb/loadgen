package main

var query6 = `
SELECT
	SUM(l_extendedprice * l_discount) AS revenue
FROM
	lineitem
WHERE
	l_shipdate >= DATE '1997-01-01'
	AND l_shipdate < DATE '1997-01-01' + INTERVAL '1' YEAR
	AND l_discount BETWEEN 0.07 - 0.01 AND 0.07 + 0.01
	AND l_quantity < 24;
`
