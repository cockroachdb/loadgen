package main

var query16 = `
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
`
