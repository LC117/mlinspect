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
from benchmarks.benchmark_utility import plot_compare, REPETITIONS, get_healthcare_csv_paths
from mlinspect.to_sql.dbms_connectors.postgresql_connector import PostgresqlConnector
from mlinspect.to_sql.dbms_connectors.umbra_connector import UmbraConnector
import time


def pipeline_code(path_patients, path_histories):
    setup_code = cleandoc("""
import warnings
import os
import pandas as pd
from mlinspect.utils import get_project_root
            """)

    test_code = f"warnings.filterwarnings(\'ignore\')\n" \
                f"COUNTIES_OF_INTEREST = [\'county2\', \'county3\']\n" \
                f"patients = pd.read_csv(\'{path_patients}\', na_values=\'?\')\n" \
                f"histories = pd.read_csv(\'{path_histories}\', na_values=\'?\')\n" \
                f"data = patients.merge(histories, on=[\'ssn\'])\n" \
                f"complications = data.groupby(\'age_group\').agg(mean_complications=(\'complications', \'mean\'))\n" \
                f"data = data.merge(complications, on=[\'age_group\'])\n" \
                f"data[\'label\'] = data[\'complications\'] > 1.2 * data[\'mean_complications\']\n" \
                f"data = data[[\'smoker\', \'last_name\', \'county\', \'num_children\', \'race\', \'income\', \'label\']]\n" \
                f"data = data[data[\'county\'].isin(COUNTIES_OF_INTEREST)]\n"

    return setup_code + "\n" + test_code


def run(pipeline_code, to_sql, dbms_connector, repetitions=1):
    inspector_result = PipelineInspector \
        .on_pipeline_from_string(pipeline_code) \
        .add_custom_monkey_patching_module(custom_monkeypatching) \
        .add_check(NoBiasIntroducedFor(["age_group", "race"])) \
        .add_check(NoIllegalFeatures()) \
        .add_check(NoMissingEmbeddings()) \
        .add_required_inspection(RowLineage(5)) \
        .add_required_inspection(MaterializeFirstOutputRows(5))

    if to_sql:
        t0 = time.time()
        inspector_result = inspector_result.execute_in_sql(reset_state=True, dbms_connector=dbms_connector)
        t1 = time.time()
    else:
        t0 = time.time()
        inspector_result = inspector_result.execute(reset_state=True)
        t1 = time.time()
    return t1 - t0


if __name__ == "__main__":

    dbms_connector_p = PostgresqlConnector(dbname="healthcare_benchmark", user="luca", password="password", port=5432,
                                         host="localhost")

    files = get_healthcare_csv_paths()
    times_to_sql_postgres = []
    times_to_sql_umbra = []
    time_default = []
    for i, (path_to_csv_pat, path_to_csv_his) in enumerate(files):

        pipe_code = pipeline_code(path_to_csv_pat, path_to_csv_his)

        times_to_sql_postgres.append(run(pipe_code, to_sql=True, dbms_connector=dbms_connector_p))
        print(f"postgres done {i}")
        dbms_connector_u = UmbraConnector(dbname="", user="postgres", password=" ", port=5433, host="/tmp/",
                                          umbra_dir=r"/home/luca/Documents/Bachelorarbeit/Umbra/umbra-students")

        times_to_sql_umbra.append(run(pipe_code, to_sql=True, dbms_connector=dbms_connector_u))
        print(f"umbra done {i}")
        del dbms_connector_u


        time_default.append(run(pipe_code, to_sql=False, dbms_connector=None))
        print(f"default done {i}")
    print(times_to_sql_postgres)
    print(times_to_sql_umbra)
    print(time_default)
