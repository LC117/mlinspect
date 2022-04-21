import pathlib
import time
from inspect import cleandoc
from mlinspect.to_sql.dbms_connectors.umbra_connector import UmbraConnector
from related_SQL.dbms_connectors.pandas_connector import PandasConnector
from mlinspect.to_sql.dbms_connectors.postgresql_connector import PostgresqlConnector
from benchmarks.benchmark_utility import plot_compare, ROOT_DIR
from benchmarks.healthcare_utility import get_healthcare_pipe_code_orig, get_healthcare_csv_paths
from mlinspect import PipelineInspector

REPETITIONS = [(10 ** i) for i in range(2, 5, 1)]
UMBRA_DIR = r"/home/luca/Documents/Bachelorarbeit/umbra-students"

def get_healthcare_SQL_str(pipeline_code, mode, materialize):
    PipelineInspector \
        .on_pipeline_from_string(pipeline_code) \
        .execute_in_sql(dbms_connector=None, mode=mode, materialize=materialize)

    setup_file = \
        pathlib.Path(r"/home/luca/Documents/Bachelorarbeit/mlinspect/mlinspect/to_sql/generated_code/create_table.sql")
    test_file = \
        pathlib.Path(r"/home/luca/Documents/Bachelorarbeit/mlinspect/mlinspect/to_sql/generated_code/pipeline.sql")

    with setup_file.open("r") as file:
        setup_code = file.read()

    with test_file.open("r") as file:
        test_code = file.read()

    return setup_code, test_code


if __name__ == "__main__":

    files = get_healthcare_csv_paths()

    umbra_times = []
    postgres_times = []
    pandas_times = []

    repetitions = 10

    postgres = PostgresqlConnector(dbname="healthcare_benchmark", user="luca", password="password", port=5432,
                                   host="localhost")
    pandas = PandasConnector()

    for i, (path_to_csv_his, path_to_csv_pat) in enumerate(files):
        print(f"ITERATION: {i} - for table size of: {REPETITIONS[i]}")

        setup_code_orig, test_code_orig = get_healthcare_pipe_code_orig(path_to_csv_his, path_to_csv_pat)

        setup_code, test_code = get_healthcare_SQL_str(setup_code_orig + "\n" + test_code_orig, mode="CTE",
                                                       materialize=False)

        ################################################################################################################
        # time Umbra:
        umbra = UmbraConnector(dbname="", user="postgres", password=" ", port=5433, host="/tmp/", umbra_dir=UMBRA_DIR)
        umbra.run(setup_code)
        umbra_times.append(umbra.benchmark_run(test_code, repetitions=repetitions))

        ################################################################################################################
        # time Postgres:
        postgres.run(setup_code)
        postgres_times.append(postgres.benchmark_run(test_code, repetitions=repetitions))

        ################################################################################################################
        # time Pandas:
        pandas_times.append(pandas.benchmark_run(pandas_code=test_code_orig, setup_code=setup_code_orig,
                                                 repetitions=repetitions))
        ################################################################################################################

    print(f"Plotting..")
    names = ["Umbra", "Postgresql", "Pandas"]
    title = "healthcare_pandas_compare"
    table = [umbra_times, postgres_times, pandas_times]
    plot = plot_compare(title, REPETITIONS, all_y=table, all_y_names=names, save=True)
    plot.show()
    plot.waitforbuttonpress()
