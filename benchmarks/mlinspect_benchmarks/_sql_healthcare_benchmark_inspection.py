import sys
import psrecord
import os
import timeit
from mlinspect.utils import get_project_root
from mlinspect import PipelineInspector
from mlinspect.inspections import RowLineage, MaterializeFirstOutputRows
from mlinspect.checks import NoBiasIntroducedFor, NoIllegalFeatures
from demo.feature_overview.no_missing_embeddings import NoMissingEmbeddings
from inspect import cleandoc
from example_pipelines.healthcare import custom_monkeypatching
from benchmarks.benchmark_utility import plot_compare, REPETITIONS
from benchmarks.healthcare_utility import get_healthcare_pipe_code_orig, get_healthcare_csv_paths
from mlinspect.to_sql.dbms_connectors.postgresql_connector import PostgresqlConnector
from mlinspect.to_sql.dbms_connectors.umbra_connector import UmbraConnector

UMBRA_DIR = r"/home/luca/Documents/Bachelorarbeit/umbra-students"
UMBRA_USER = "postgres"
UMBRA_PW = " "
POSTGRES_USER = "luca"
POSTGRES_PW = "password"
POSTGRES_DB = "healthcare_benchmark"
UMBRA_DB = ""
UMBRA_PORT = 5433
POSTGRES_PORT = 5432
UMBRA_HOST = "/tmp/"
POSTGRES_HOST = "localhost"

import timeit
from inspect import cleandoc
import matplotlib.pyplot as plt

DO_CLEANUP = True
SIZES = [(10 ** i) for i in range(2, 5, 1)]
BENCH_REP = 10

# DBMS related:
UMBRA_DIR = r"/home/luca/Documents/Bachelorarbeit/umbra-students"
UMBRA_USER = "postgres"
UMBRA_PW = " "
UMBRA_DB = ""
UMBRA_PORT = 5433
UMBRA_HOST = "/tmp/"

POSTGRES_USER = "luca"
POSTGRES_PW = "password"
POSTGRES_DB = "healthcare_benchmark"
POSTGRES_PORT = 5432
POSTGRES_HOST = "localhost"


def get_inspection_code(pipeline_code, to_sql, dbms_connector, one_run=False):
    setup_code = cleandoc(f"""
from inspect import cleandoc
from mlinspect.utils import get_project_root
from mlinspect import PipelineInspector
from mlinspect.inspections import RowLineage, MaterializeFirstOutputRows
from mlinspect.checks import NoBiasIntroducedFor, NoIllegalFeatures
from demo.feature_overview.no_missing_embeddings import NoMissingEmbeddings
from inspect import cleandoc
from example_pipelines.healthcare import custom_monkeypatching
from mlinspect.to_sql.dbms_connectors.postgresql_connector import PostgresqlConnector
from mlinspect.to_sql.dbms_connectors.umbra_connector import UmbraConnector
from mlinspect import PipelineInspector

dbms_connector_u = UmbraConnector(dbname=\'{UMBRA_DB}\', user=\'{UMBRA_USER}\', password=\'{UMBRA_PW}\',
    port={UMBRA_PORT}, host=\'{UMBRA_HOST}\', umbra_dir= r\'{UMBRA_DIR}\')
dbms_connector_p = PostgresqlConnector(dbname=\'{POSTGRES_DB}\', user=\'{POSTGRES_USER}\',
    password=\'{POSTGRES_PW}\', port={POSTGRES_PORT}, host=\'{POSTGRES_HOST}\')

pipeline_code = cleandoc(f\"\"\"{pipeline_code}\"\"\")

pipeline_inspector = PipelineInspector.on_pipeline_from_string(pipeline_code) \\
    .add_custom_monkey_patching_module(custom_monkeypatching) \\
    .add_check(NoBiasIntroducedFor([\'age_group\', \'race\'])) \\
    .add_check(NoIllegalFeatures()) \\
    .add_check(NoMissingEmbeddings()) \\
    .add_required_inspection(RowLineage(5)) \\
    .add_required_inspection(MaterializeFirstOutputRows(5))
    """)
    if to_sql:
        return setup_code, f"pipeline_inspector.execute_in_sql(dbms_connector={dbms_connector}, " \
                           f"sql_one_run={one_run}, mode=\'CTE\')"

    return setup_code, f"pipeline_inspector.execute()"


def run(pipeline_code, to_sql=False, dbms_connector=None, one_run=False):
    setup_code, test_code = get_inspection_code(pipeline_code, to_sql, dbms_connector, one_run)
    if to_sql:
        result = []
        for _ in range(BENCH_REP):
            # This special case is necessary to deduct the time for dropping the existing tables and views!
            result.append(timeit.timeit(test_code, setup=setup_code, number=1) * 1000)  # in s
        return sum(result) / BENCH_REP

    return (timeit.timeit(test_code, setup=setup_code, number=BENCH_REP) / BENCH_REP) * 1000  # in s


def pipeline_inspection_benchmark(display_hardware_usage: bool = False):
    pandas_times = []
    postgres_times = []
    umbra_times = []
    for sql_one_run in [False]:
        for i, (path_to_csv_pat, path_to_csv_his) in enumerate(get_healthcare_csv_paths()):
            setup_code, test_code = get_healthcare_pipe_code_orig(path_to_csv_pat, path_to_csv_his)
            pipe_code = setup_code + "\n" + test_code

            print(f"Running pandas...  -- size {SIZES[i]}")
            pandas_times.append(run(pipe_code, to_sql=False, dbms_connector=None, one_run=sql_one_run))
            print(f"Running postgres...  -- size {SIZES[i]}")
            postgres_times.append(run(pipe_code, to_sql=True, dbms_connector="dbms_connector_p", one_run=sql_one_run))
            print(f"Running umbra... -- size {SIZES[i]}")
            umbra_times.append(run(pipe_code, to_sql=True, dbms_connector="dbms_connector_u", one_run=sql_one_run))

            # psrecord 1330 --log activity.txt --interval 2
            sys.argv = ["psrecord", f"{pid}", "--log=activity.txt", "--plot plot.png", "--interval 1", "--include-children"]
            psrecord.main()

        names = ["Pandas", "Postgresql", "Umbra"]
        title = "healthcare_pandas_compare"
        table = [pandas_times, postgres_times, umbra_times]
        plot = plot_compare(title, SIZES, all_y=table, all_y_names=names, save=True)
        plot.show()


if __name__ == "__main__":
    pipeline_inspection_benchmark()
