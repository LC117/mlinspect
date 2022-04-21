import sys
import psrecord

import subprocess
import time
import fcntl
import os
import pandas
import tempfile
import csv

import tracemalloc

import numpy as np

from mlinspect.utils import get_project_root
import pandas as pd
from mlinspect import PipelineInspector, OperatorType
from mlinspect.inspections import HistogramForColumns, RowLineage, MaterializeFirstOutputRows
from mlinspect.checks import NoBiasIntroducedFor, NoIllegalFeatures
from demo.feature_overview.no_missing_embeddings import NoMissingEmbeddings
from inspect import cleandoc
from example_pipelines.healthcare import custom_monkeypatching
from mlinspect.to_sql.dbms_connectors.postgresql_connector import PostgresqlConnector
import time
from IPython.display import display
from mlinspect.to_sql.dbms_connectors.umbra_connector import UmbraConnector

from mlinspect.to_sql.dbms_connectors.postgresql_connector import PostgresqlConnector
from mlinspect.to_sql.dbms_connectors.umbra_connector import UmbraConnector


def tracing_start():
    tracemalloc.stop()
    print("nTracing Status : ", tracemalloc.is_tracing())
    tracemalloc.start()
    print("Tracing Status : ", tracemalloc.is_tracing())


def tracing_mem():
    first_size, first_peak = tracemalloc.get_traced_memory()
    peak = first_peak / (1024 * 1024)
    print("Peak Size in MB - ", peak)




HEALTHCARE_FILE_PY = os.path.join(str(get_project_root()), "example_pipelines", "healthcare", "healthcare.py")

inspector_result = PipelineInspector \
    .on_pipeline_from_py_file(HEALTHCARE_FILE_PY) \
    .add_custom_monkey_patching_module(custom_monkeypatching) \
    .add_check(NoBiasIntroducedFor(["age_group", "race"])) \
    .add_check(NoIllegalFeatures()) \
    .add_check(NoMissingEmbeddings()) \
    .add_required_inspection(RowLineage(5)) \
    .add_required_inspection(MaterializeFirstOutputRows(5))

# print("Pandas:")
# tracing_start()
# inspector_result = inspector_result.execute( )
# tracing_mem()



print("Postgres:")
dbms_connector = PostgresqlConnector(dbname="healthcare_benchmark", user="luca", password="password",
                                     port=5432,
                                     host="localhost")
tracing_start()
inspector_result.execute_in_sql(dbms_connector=dbms_connector, sql_one_run=False,
                                                   mode="CTE", materialize=False, row_wise=False)
tracing_mem()




print("Umbra:")
umbra_path = r"/home/luca/Documents/Bachelorarbeit/umbra-students"
dbms_connector = UmbraConnector(dbname="", user="postgres", password=" ", port=5433, host="/tmp/",
                                  umbra_dir=umbra_path)

tracing_start()
inspector_result.execute_in_sql(dbms_connector=dbms_connector, sql_one_run=False,
                                                   mode="CTE", materialize=False, row_wise=False)

tracing_mem()
