package main

var query11 = `
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
`
