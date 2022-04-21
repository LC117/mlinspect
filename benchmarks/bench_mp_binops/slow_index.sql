WITH data_b AS (
	SELECT b, index_
	FROM data
),
data_c AS (
	SELECT c, index_
	FROM data
),
data_c_mul AS (
	SELECT (c * 1.2) AS c_mul, index_
	FROM data_c
),
data_b_ge_data_c AS (
	SELECT (l.b > r.c_mul) AS b_ge_c_mul, l.index_
	FROM data_b l, data_c_mul r
	WHERE l.index_ = r.index_
),
result AS (
	SELECT l.data_f, l.b, l.c, r.b_ge_c_mul AS label, l.index_
	FROM  data l, data_b_ge_data_c r
	WHERE l.index_ = r.index_
)

select * from result