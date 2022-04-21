CREATE TABLE patients_101_mlinid0 (
	"id" INT,
	"first_name" VARCHAR(100),
	"last_name" VARCHAR(100),
	"race" VARCHAR(100),
	"county" VARCHAR(100),
	"num_children" INT,
	"income" FLOAT,
	"age_group" VARCHAR(100),
	"ssn" VARCHAR(100)
);

COPY patients_101_mlinid0("id", "first_name", "last_name", "race", "county", "num_children", "income", "age_group", "ssn") FROM '/home/luca/Documents/BA_saved/For_Publication/my_repo/mlinspect/example_pipelines/healthcare/patients.csv' WITH (DELIMITER ',', NULL '?', FORMAT CSV, HEADER TRUE);


CREATE TABLE histories_102_mlinid1 (
	"smoker" VARCHAR(100),
	"complications" INT,
	"ssn" VARCHAR(100)
);

COPY histories_102_mlinid1("smoker", "complications", "ssn") FROM '/home/luca/Documents/BA_saved/For_Publication/my_repo/mlinspect/example_pipelines/healthcare/histories.csv' WITH (DELIMITER ',', NULL '?', FORMAT CSV, HEADER TRUE);


