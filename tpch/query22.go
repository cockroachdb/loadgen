package main

var query22 = `
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
`
