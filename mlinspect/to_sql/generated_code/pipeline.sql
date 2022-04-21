CREATE VIEW patients_101_mlinid0_ctid AS (
	SELECT *, ctid AS patients_101_mlinid0_ctid
	FROM patients_101_mlinid0
);
CREATE VIEW histories_102_mlinid1_ctid AS (
	SELECT *, ctid AS histories_102_mlinid1_ctid
	FROM histories_102_mlinid1
);
CREATE VIEW block_mlinid2_103 AS (
	SELECT tb1."id", tb1."first_name", tb1."last_name", tb1."race", tb1."county", tb1."num_children", tb1."income", tb1."age_group", tb1."ssn", tb1.patients_101_mlinid0_ctid, tb2."smoker", tb2."complications", tb2.histories_102_mlinid1_ctid
	FROM patients_101_mlinid0_ctid tb1 
	INNER JOIN histories_102_mlinid1_ctid tb2 ON tb1."ssn" = tb2."ssn"
);
CREATE VIEW block_mlinid3_104 AS (
	SELECT "age_group", AVG("complications") AS "mean_complications" 
	FROM block_mlinid2_103
	GROUP BY "age_group"
);
CREATE VIEW block_mlinid4_105 AS (
	SELECT tb1."id", tb1."first_name", tb1."last_name", tb1."race", tb1."county", tb1."num_children", tb1."income", tb1."age_group", tb1."ssn", tb1."smoker", tb1."complications", tb1.histories_102_mlinid1_ctid, tb1.patients_101_mlinid0_ctid, tb2."mean_complications"
	FROM block_mlinid2_103 tb1 
	INNER JOIN block_mlinid3_104 tb2 ON tb1."age_group" = tb2."age_group"
);
CREATE VIEW block_mlinid5_106 AS (
	SELECT "complications", histories_102_mlinid1_ctid, patients_101_mlinid0_ctid
	FROM block_mlinid4_105
);
CREATE VIEW block_mlinid6_107 AS (
	SELECT "mean_complications", histories_102_mlinid1_ctid, patients_101_mlinid0_ctid
	FROM block_mlinid4_105
);
CREATE VIEW block_mlinid7_109 AS (
	SELECT 1.2 * "mean_complications" AS op_108, histories_102_mlinid1_ctid, patients_101_mlinid0_ctid
	FROM block_mlinid4_105
);
CREATE VIEW block_mlinid8_111 AS (
	SELECT ("complications" > 1.2 * "mean_complications") AS op_110, histories_102_mlinid1_ctid, patients_101_mlinid0_ctid
	FROM block_mlinid4_105
);
CREATE VIEW block_mlinid9_112 AS (
	SELECT *, "complications" > 1.2 * "mean_complications" AS label
	FROM block_mlinid4_105
);
CREATE VIEW block_mlinid10_113 AS (
	SELECT "smoker", "last_name", "county", "num_children", "race", "income", "label", histories_102_mlinid1_ctid, patients_101_mlinid0_ctid
	FROM block_mlinid9_112
);
CREATE VIEW block_mlinid11_114 AS (
	SELECT "county", histories_102_mlinid1_ctid, patients_101_mlinid0_ctid
	FROM block_mlinid10_113
);
CREATE VIEW block_mlinid12_115 AS (
	SELECT "county" IN ('county2', 'county3'), histories_102_mlinid1_ctid, patients_101_mlinid0_ctid
	FROM block_mlinid10_113
);
CREATE VIEW block_mlinid13_116 AS (
	SELECT * 
	FROM block_mlinid10_113 
	WHERE "county" IN ('county2', 'county3')
);
CREATE VIEW block_train_split_mlinid14_117 AS (
	WITH row_num_mlinspect_split AS(
		SELECT *, (ROW_NUMBER() OVER()) AS row_number_mlinspect
		FROM (SELECT * FROM block_mlinid13_116 ORDER BY RANDOM()) block_mlinid13_116
	)
	SELECT *
	FROM row_num_mlinspect_split
	WHERE row_number_mlinspect < 0.75 * (SELECT COUNT(*) FROM block_mlinid13_116)
);
CREATE VIEW block_test_split_mlinid14_118 AS (
	WITH row_num_mlinspect_split AS(
		SELECT *, (ROW_NUMBER() OVER()) AS row_number_mlinspect
		FROM (SELECT * FROM block_mlinid13_116 ORDER BY RANDOM()) block_mlinid13_116
	)
	SELECT *
	FROM row_num_mlinspect_split
	WHERE row_number_mlinspect >= 0.75 * (SELECT COUNT(*) FROM block_mlinid13_116)
);
CREATE VIEW block_mlinid16_119 AS (
	SELECT "label", histories_102_mlinid1_ctid, patients_101_mlinid0_ctid
	FROM block_train_split_mlinid14_117
);
CREATE MATERIALIZED VIEW block_train_split_mlinid14_117_materialized AS (
	WITH row_num_mlinspect_split AS(
		SELECT *, (ROW_NUMBER() OVER()) AS row_number_mlinspect
		FROM (SELECT * FROM block_mlinid13_116 ORDER BY RANDOM()) block_mlinid13_116
	)
	SELECT *
	FROM row_num_mlinspect_split
	WHERE row_number_mlinspect < 0.75 * (SELECT COUNT(*) FROM block_mlinid13_116)
);
CREATE MATERIALIZED VIEW block_impute_fit_120_most_frequent_121_materialized AS (
	WITH counts_help AS (
		SELECT "smoker", COUNT(*) AS count
		FROM block_train_split_mlinid14_117_materialized 
		GROUP BY "smoker"
	)
	SELECT "smoker" AS most_frequent 
	FROM counts_help
	WHERE counts_help.count = (SELECT MAX(count) FROM counts_help)
	LIMIT 1
);
CREATE MATERIALIZED VIEW block_impute_fit_122_most_frequent_123_materialized AS (
	WITH counts_help AS (
		SELECT "county", COUNT(*) AS count
		FROM block_train_split_mlinid14_117_materialized 
		GROUP BY "county"
	)
	SELECT "county" AS most_frequent 
	FROM counts_help
	WHERE counts_help.count = (SELECT MAX(count) FROM counts_help)
	LIMIT 1
);
CREATE MATERIALIZED VIEW block_impute_fit_124_most_frequent_125_materialized AS (
	WITH counts_help AS (
		SELECT "race", COUNT(*) AS count
		FROM block_train_split_mlinid14_117_materialized 
		GROUP BY "race"
	)
	SELECT "race" AS most_frequent 
	FROM counts_help
	WHERE counts_help.count = (SELECT MAX(count) FROM counts_help)
	LIMIT 1
);
CREATE VIEW block_impute_mlinid18_126 AS (
	SELECT 
		COALESCE("smoker", (SELECT * FROM block_impute_fit_120_most_frequent_121_materialized)) AS "smoker",
		COALESCE("county", (SELECT * FROM block_impute_fit_122_most_frequent_123_materialized)) AS "county",
		"num_children",
		COALESCE("race", (SELECT * FROM block_impute_fit_124_most_frequent_125_materialized)) AS "race",
		"income",
		histories_102_mlinid1_ctid, patients_101_mlinid0_ctid 
	FROM block_train_split_mlinid14_117_materialized
);
CREATE MATERIALIZED VIEW block_one_hot_fit_127_128_materialized AS (
	SELECT "smoker", 
	(array_fill(0, ARRAY["rank" - 1]) || 1 ) || array_fill(0, ARRAY[ CAST((select COUNT(distinct("smoker")) FROM block_impute_mlinid18_126) AS int) - ("rank")]) AS "smoker_one_hot" 
		FROM (
		SELECT "smoker", CAST(ROW_NUMBER() OVER() AS int) AS "rank" 
		FROM (SELECT distinct("smoker") FROM block_impute_mlinid18_126) oh
	) one_hot_help
);
CREATE MATERIALIZED VIEW block_one_hot_fit_129_130_materialized AS (
	SELECT "county", 
	(array_fill(0, ARRAY["rank" - 1]) || 1 ) || array_fill(0, ARRAY[ CAST((select COUNT(distinct("county")) FROM block_impute_mlinid18_126) AS int) - ("rank")]) AS "county_one_hot" 
		FROM (
		SELECT "county", CAST(ROW_NUMBER() OVER() AS int) AS "rank" 
		FROM (SELECT distinct("county") FROM block_impute_mlinid18_126) oh
	) one_hot_help
);
CREATE MATERIALIZED VIEW block_one_hot_fit_131_132_materialized AS (
	SELECT "race", 
	(array_fill(0, ARRAY["rank" - 1]) || 1 ) || array_fill(0, ARRAY[ CAST((select COUNT(distinct("race")) FROM block_impute_mlinid18_126) AS int) - ("rank")]) AS "race_one_hot" 
		FROM (
		SELECT "race", CAST(ROW_NUMBER() OVER() AS int) AS "rank" 
		FROM (SELECT distinct("race") FROM block_impute_mlinid18_126) oh
	) one_hot_help
);
CREATE VIEW block_onehot_mlinid19_133 AS (
	SELECT 
		"smoker_one_hot" AS "smoker",
		"county_one_hot" AS "county",
		"num_children",
		"race_one_hot" AS "race",
		"income",
		histories_102_mlinid1_ctid, patients_101_mlinid0_ctid 
	FROM block_one_hot_fit_127_128_materialized, block_one_hot_fit_129_130_materialized, block_one_hot_fit_131_132_materialized, block_impute_mlinid18_126
	WHERE
	 	block_impute_mlinid18_126."smoker" = block_one_hot_fit_127_128_materialized."smoker" AND 
		block_impute_mlinid18_126."county" = block_one_hot_fit_129_130_materialized."county" AND 
		block_impute_mlinid18_126."race" = block_one_hot_fit_131_132_materialized."race"
);
CREATE MATERIALIZED VIEW block_std_scalar_fit_134_std_avg_135_materialized AS (
	SELECT (SELECT AVG("num_children") FROM block_train_split_mlinid14_117_materialized) AS avg_col_std_scal,(SELECT STDDEV_POP("num_children") FROM block_train_split_mlinid14_117_materialized) AS std_dev_col_std_scal
);
CREATE MATERIALIZED VIEW block_std_scalar_fit_136_std_avg_137_materialized AS (
	SELECT (SELECT AVG("income") FROM block_train_split_mlinid14_117_materialized) AS avg_col_std_scal,(SELECT STDDEV_POP("income") FROM block_train_split_mlinid14_117_materialized) AS std_dev_col_std_scal
);
CREATE VIEW block_stdscaler_mlinid20_138 AS (
	SELECT 
		"smoker",
		"county",
		(("num_children" - (SELECT avg_col_std_scal FROM block_std_scalar_fit_134_std_avg_135_materialized)) / (SELECT std_dev_col_std_scal FROM block_std_scalar_fit_134_std_avg_135_materialized)) AS "num_children",
		"race",
		(("income" - (SELECT avg_col_std_scal FROM block_std_scalar_fit_136_std_avg_137_materialized)) / (SELECT std_dev_col_std_scal FROM block_std_scalar_fit_136_std_avg_137_materialized)) AS "income",
		histories_102_mlinid1_ctid, patients_101_mlinid0_ctid 
	FROM block_train_split_mlinid14_117_materialized
);
CREATE VIEW block_column_transformer_lvl1_140 AS (
	WITH lvl0_column_transformer_139 AS (
		SELECT 
			COALESCE("smoker", (SELECT * FROM block_impute_fit_120_most_frequent_121_materialized)) AS "smoker",
			COALESCE("county", (SELECT * FROM block_impute_fit_122_most_frequent_123_materialized)) AS "county",
			COALESCE("race", (SELECT * FROM block_impute_fit_124_most_frequent_125_materialized)) AS "race",
			(("num_children" - (SELECT avg_col_std_scal FROM block_std_scalar_fit_134_std_avg_135_materialized)) / (SELECT std_dev_col_std_scal FROM block_std_scalar_fit_134_std_avg_135_materialized)) AS "num_children",
			(("income" - (SELECT avg_col_std_scal FROM block_std_scalar_fit_136_std_avg_137_materialized)) / (SELECT std_dev_col_std_scal FROM block_std_scalar_fit_136_std_avg_137_materialized)) AS "income",
			histories_102_mlinid1_ctid, patients_101_mlinid0_ctid 
		FROM block_train_split_mlinid14_117
	)
	SELECT 
		"smoker_one_hot" AS "smoker",
		"county_one_hot" AS "county",
		"race_one_hot" AS "race",
		"num_children",
		"income",
		histories_102_mlinid1_ctid, patients_101_mlinid0_ctid 
	FROM block_one_hot_fit_131_132_materialized, lvl0_column_transformer_139, block_one_hot_fit_127_128_materialized, block_one_hot_fit_129_130_materialized
	WHERE
		lvl0_column_transformer_139."county" = block_one_hot_fit_129_130_materialized."county" AND 
		lvl0_column_transformer_139."race" = block_one_hot_fit_131_132_materialized."race" AND 
		lvl0_column_transformer_139."smoker" = block_one_hot_fit_127_128_materialized."smoker"
);

/*ATTENTION: MODEL FIT: TRAIN LABELS
SELECT "smoker", "county", "race", "num_children", "income" FROM block_column_transformer_lvl1_140 ORDER BY index_mlinspect;
*/

/*ATTENTION: MODEL FIT: TEST LABELS
SELECT "label" FROM block_mlinid16_119 ORDER BY index_mlinspect;
*/
CREATE VIEW block_mlinid26_141 AS (
	SELECT "label", histories_102_mlinid1_ctid, patients_101_mlinid0_ctid
	FROM block_test_split_mlinid14_118
);
CREATE MATERIALIZED VIEW block_test_split_mlinid14_118_materialized AS (
	WITH row_num_mlinspect_split AS(
		SELECT *, (ROW_NUMBER() OVER()) AS row_number_mlinspect
		FROM (SELECT * FROM block_mlinid13_116 ORDER BY RANDOM()) block_mlinid13_116
	)
	SELECT *
	FROM row_num_mlinspect_split
	WHERE row_number_mlinspect >= 0.75 * (SELECT COUNT(*) FROM block_mlinid13_116)
);
CREATE VIEW block_impute_mlinid143_144 AS (
	SELECT 
		COALESCE("smoker", (SELECT * FROM block_impute_fit_120_most_frequent_121_materialized)) AS "smoker",
		COALESCE("county", (SELECT * FROM block_impute_fit_122_most_frequent_123_materialized)) AS "county",
		"num_children",
		COALESCE("race", (SELECT * FROM block_impute_fit_124_most_frequent_125_materialized)) AS "race",
		"income",
		histories_102_mlinid1_ctid, patients_101_mlinid0_ctid 
	FROM block_test_split_mlinid14_118_materialized
);
CREATE VIEW block_onehot_mlinid145_146 AS (
	SELECT 
		"smoker_one_hot" AS "smoker",
		"county_one_hot" AS "county",
		"num_children",
		"race_one_hot" AS "race",
		"income",
		histories_102_mlinid1_ctid, patients_101_mlinid0_ctid 
	FROM block_one_hot_fit_127_128_materialized, block_one_hot_fit_129_130_materialized, block_one_hot_fit_131_132_materialized, block_impute_mlinid143_144
	WHERE
	 	block_impute_mlinid143_144."smoker" = block_one_hot_fit_127_128_materialized."smoker" AND 
		block_impute_mlinid143_144."county" = block_one_hot_fit_129_130_materialized."county" AND 
		block_impute_mlinid143_144."race" = block_one_hot_fit_131_132_materialized."race"
);
CREATE VIEW block_stdscaler_mlinid147_148 AS (
	SELECT 
		"smoker",
		"county",
		(("num_children" - (SELECT avg_col_std_scal FROM block_std_scalar_fit_134_std_avg_135_materialized)) / (SELECT std_dev_col_std_scal FROM block_std_scalar_fit_134_std_avg_135_materialized)) AS "num_children",
		"race",
		(("income" - (SELECT avg_col_std_scal FROM block_std_scalar_fit_136_std_avg_137_materialized)) / (SELECT std_dev_col_std_scal FROM block_std_scalar_fit_136_std_avg_137_materialized)) AS "income",
		histories_102_mlinid1_ctid, patients_101_mlinid0_ctid 
	FROM block_test_split_mlinid14_118_materialized
);
CREATE VIEW block_column_transformer_lvl1_150 AS (
	WITH lvl0_column_transformer_149 AS (
		SELECT 
			COALESCE("smoker", (SELECT * FROM block_impute_fit_120_most_frequent_121_materialized)) AS "smoker",
			COALESCE("county", (SELECT * FROM block_impute_fit_122_most_frequent_123_materialized)) AS "county",
			COALESCE("race", (SELECT * FROM block_impute_fit_124_most_frequent_125_materialized)) AS "race",
			(("num_children" - (SELECT avg_col_std_scal FROM block_std_scalar_fit_134_std_avg_135_materialized)) / (SELECT std_dev_col_std_scal FROM block_std_scalar_fit_134_std_avg_135_materialized)) AS "num_children",
			(("income" - (SELECT avg_col_std_scal FROM block_std_scalar_fit_136_std_avg_137_materialized)) / (SELECT std_dev_col_std_scal FROM block_std_scalar_fit_136_std_avg_137_materialized)) AS "income",
			histories_102_mlinid1_ctid, patients_101_mlinid0_ctid 
		FROM block_test_split_mlinid14_118
	)
	SELECT 
		"smoker_one_hot" AS "smoker",
		"county_one_hot" AS "county",
		"race_one_hot" AS "race",
		"num_children",
		"income",
		histories_102_mlinid1_ctid, patients_101_mlinid0_ctid 
	FROM block_one_hot_fit_131_132_materialized, lvl0_column_transformer_149, block_one_hot_fit_127_128_materialized, block_one_hot_fit_129_130_materialized
	WHERE
		lvl0_column_transformer_149."county" = block_one_hot_fit_129_130_materialized."county" AND 
		lvl0_column_transformer_149."smoker" = block_one_hot_fit_127_128_materialized."smoker" AND 
		lvl0_column_transformer_149."race" = block_one_hot_fit_131_132_materialized."race"
);

/*ATTENTION: MODEL SCORE: INPUT DATA
SELECT "smoker", "county", "race", "num_children", "income" FROM block_column_transformer_lvl1_150 ORDER BY index_mlinspect;
*/

/*ATTENTION: MODEL SCORE: EXPECTED OUTPUT DATA
SELECT "label" FROM block_mlinid26_141 ORDER BY index_mlinspect;
*/

SELECT "label" FROM block_mlinid26_141 ORDER BY index_mlinspect