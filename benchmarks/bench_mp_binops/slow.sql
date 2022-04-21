WITH data_b AS (
	SELECT b
	FROM data
),
data_c AS (
	SELECT c
	FROM data
),
data_c_mul AS (
	SELECT (c * 1.2) AS c_mul
	FROM data_c
),
data_b_gt_data_c AS (
	SELECT (l.b > r.c_mul) AS b_gt_c_mul
	FROM (SELECT *, ROW_NUMBER() OVER() FROM data_b) l, (SELECT *, ROW_NUMBER() OVER() FROM data_c_mul) r
	WHERE l.row_number = r.row_number
),
result AS (
	SELECT l.*, r.b_gt_c_mul AS label
	FROM (SELECT *, ROW_NUMBER() OVER() FROM data) l, (SELECT *, ROW_NUMBER() OVER() FROM data_b_gt_data_c) r
	WHERE l.row_number = r.row_number
)

select * from result