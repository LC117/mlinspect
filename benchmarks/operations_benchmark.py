"""
This benchmark compares the different frequently used operations for preprocessing realized in Umbra, Pandas, and
PostgreSQL.
"""
import timeit
from inspect import cleandoc
from mlinspect.to_sql.dbms_connectors.postgresql_connector import PostgresqlConnector
from mlinspect.to_sql.dbms_connectors.umbra_connector import UmbraConnector
from related_SQL.dbms_connectors.pandas_connector import PandasConnector
from benchmarks.benchmark_utility import REPETITIONS, REPETITIONS_EXT
from benchmark_utility import plot_compare, ROOT_DIR


class Join:
    """
    Code for simple join.
    """
    @staticmethod
    def get_name():
        return "Join"

    @staticmethod
    def get_pandas_code(path_1, path_2, join_attr, na_val="?"):
        pandas_code = cleandoc(f"""
        df1 = pandas.read_csv(r\"{path_1}\", na_values=[\"{na_val}\"])
        df2 = pandas.read_csv(r\"{path_2}\", na_values=[\"{na_val}\"])
        result = df1.merge(df2, on=['{join_attr}'])
        """)
        return pandas_code

    @staticmethod
    def get_sql_code(ds_1, ds_2, join_attr):
        sql_code = cleandoc(f"""
        SELECT * 
        FROM {ds_1}
        INNER JOIN {ds_2} ON {ds_1}.{join_attr} = {ds_2}.{join_attr}
        """)
        return sql_code


class Projection:
    """
    Won't be used!!
    """
    @staticmethod
    def get_name():
        return "Projection"

    @staticmethod
    def get_pandas_code(path, attr, na_val="?"):
        pandas_code = cleandoc(f"""
        df = pandas.read_csv(r\"{path}\", na_values=[\"{na_val}\"])
        result = df[[\"{attr}\"]]
        """)
        return pandas_code

    @staticmethod
    def get_sql_code(ds, attr):
        sql_code = cleandoc(f"""
        SELECT count({attr})
        FROM {ds}
        """)
        return sql_code


class Selection:
    """
    Code for selection benchmark, with aggregation.
    """

    @staticmethod
    def get_name():
        return "Selection"

    @staticmethod
    def get_pandas_code(path, attr, cond="", value="", na_val="?"):
        pandas_code = cleandoc(f"""
        df = pandas.read_csv(r\"{path}\", na_values=[\"{na_val}\"])
        result = df.loc[lambda df: (df['{attr}'] {cond} {value}), :]
        """)
        return pandas_code

    @staticmethod
    def get_sql_code(ds, attr, cond="", value=""):
        sql_code = cleandoc(f"""
        SELECT *
        FROM {ds}
        WHERE {attr} {cond} {value}
        """)
        return sql_code


class GroupBy:
    """
    Code for group by benchmark.
    """

    @staticmethod
    def get_name():
        return "GroupBy"

    @staticmethod
    def get_pandas_code(path, attr1, attr2, op, na_val="?"):
        pandas_code = cleandoc(f"""
        df = pandas.read_csv(r\"{path}\", na_values=[\"{na_val}\"])
        complications = df.groupby('{attr1}').agg(mean_complications=('{attr2}', '{op}'))
        """)
        return pandas_code

    @staticmethod
    def get_sql_code(ds, attr1, attr2, op):
        # Here we don't need a count, as smoker is boolean and we will only need to print 3 values: null, True, False
        sql_code = cleandoc(f"""
        SELECT {attr1}, {op}({attr2})
        FROM {ds}
        GROUP BY {attr1}
        """)
        return sql_code


def benchmark_of_basic_operations_healthcare(umbra_dir):
    """
    Based on mlinspect benchmarks.
    """
    t1_name = "histories"
    t2_name = "patients"

    operations = ["Join", "Select", "Project", "GroupBy"]

    files = []
    for i in REPETITIONS:
        path_table1 = ROOT_DIR / f"data_generation/generated_csv/healthcare_histories_generated_{i}.csv"
        path_table2 = ROOT_DIR / f"data_generation/generated_csv/healthcare_patients_generated_{i}.csv"
        files.append((path_table1, path_table2))

    umbra_times = [[] for _ in operations]
    postgres_times = [[] for _ in operations]
    pandas_times = [[] for _ in operations]

    postgres = PostgresqlConnector(dbname="healthcare_benchmark", user="luca", password="password", port=5432,
                                   host="localhost")
    pandas = PandasConnector()

    for i, (table1, table2) in enumerate(files):

        umbra = UmbraConnector(dbname="", user="postgres", password=" ", port=5433, host="/tmp/",
                               umbra_dir=umbra_dir)

        umbra.add_csv(table_name=t2_name, path_to_csv=table2, null_symbols=["?"], delimiter=",", header=True)
        umbra.add_csv(table_name=t1_name, path_to_csv=table1, null_symbols=["?"], delimiter=",", header=True)

        postgres.add_csv(table_name=t2_name, path_to_csv=table2, null_symbols=["?"], delimiter=",", header=True)
        postgres.add_csv(table_name=t1_name, path_to_csv=table1, null_symbols=["?"], delimiter=",", header=True)

        print(f"ITERATION: {i} - for table size of: {10 ** (i + 2)}")
        repetitions = 10

        input_join = t1_name, t2_name, "ssn"
        umbra_times[0].append(umbra.benchmark_run(Join.get_sql_code(*input_join), repetitions))
        postgres_times[0].append(postgres.benchmark_run(Join.get_sql_code(*input_join), repetitions))
        pandas_times[0].append(
            pandas.benchmark_run(Join.get_pandas_code(table1, table2, "ssn"), repetitions=repetitions))

        input_sel = t1_name, "complications", ">", "5"
        umbra_times[1].append(umbra.benchmark_run(Selection.get_sql_code(*input_sel), repetitions))
        postgres_times[1].append(postgres.benchmark_run(Selection.get_sql_code(*input_sel), repetitions))
        pandas_times[1].append(
            pandas.benchmark_run(Selection.get_pandas_code(table1, "complications", ">", "5"),
                                  repetitions=repetitions))

        input_project = t1_name, "smoker"
        umbra_times[2].append(umbra.benchmark_run(Projection.get_sql_code(*input_project), repetitions))
        postgres_times[2].append(postgres.benchmark_run(Projection.get_sql_code(*input_project), repetitions))
        pandas_times[2].append(
            pandas.benchmark_run(Projection.get_pandas_code(table1, "smoker"), repetitions=repetitions))

        input_project = t1_name, "smoker", "complications", "AVG"
        umbra_times[3].append(umbra.benchmark_run(GroupBy.get_sql_code(*input_project), repetitions))
        postgres_times[3].append(postgres.benchmark_run(GroupBy.get_sql_code(*input_project), repetitions))
        pandas_times[3].append(
            pandas.benchmark_run(GroupBy.get_pandas_code(table1, "smoker", "complications", "mean"),
                                  repetitions=repetitions))
        # in the end we have 3 lists == [[*joins*][*selections*][*projections*]]

    names = ["Umbra", "Postgresql", "Pandas"]
    for i, title in enumerate(operations):
        table = [umbra_times[i], postgres_times[i], pandas_times[i]]
        plot = plot_compare(f"{title} performance comparison:", REPETITIONS, all_y=table, all_y_names=names, save=True)
        # plot.show()
        # plot.waitforbuttonpress()


# def benchmark_of_basic_operations_simple_fake_data():
#     """
#     Based on mlinspect benchmarks.
#     """
#     t1_name = "table1"
#     t2_name = "table2"
#
#     files = []
#     for i in REPETITIONS:
#         path_table1 = ROOT_DIR + f"/data_generation/generated_csv/simple_fake_data{i}.csv"
#         path_table2 = ROOT_DIR + f"/data_generation/generated_csv/simple_fake_data_second{i}.csv"
#         files.append((path_table1, path_table2))
#
#     umbra_times = [[] for _ in range(3)]
#     postgres_times = [[] for _ in range(3)]
#     pandas_times = [[] for _ in range(3)]
#
#     for i, (table1, table2) in enumerate(files):
#         umbra = UmbraSession()
#         postgres = PostgresqlSession()
#         pandas = PandasSession()
#
#         umbra.add_csv_to_db("table1", table1)
#         umbra.add_csv_to_db("table2", table2)
#         postgres.add_csv_to_db("table1", table1)
#         postgres.add_csv_to_db("table2", table2)
#
#         print(f"ITERATION: {i} - for table size of: {10 ** (i + 2)}")
#         repetitions = 3
#
#         input_join = t1_name, t2_name, "id"
#         umbra_times[0].append(umbra.benchmark_query_code(Join.get_sql_code(*input_join), repetitions))
#         postgres_times[0].append(postgres.benchmark_query_code(Join.get_sql_code(*input_join), repetitions))
#         pandas_times[0].append(pandas.benchmark_code(Join.get_pandas_code(table1, table2, "id"), repetitions))
#
#         input_sel = t1_name, "a", ">", "30"
#         umbra_times[1].append(umbra.benchmark_query_code(Selection.get_sql_code(*input_sel), repetitions))
#         postgres_times[1].append(postgres.benchmark_query_code(Selection.get_sql_code(*input_sel), repetitions))
#         pandas_times[1].append(pandas.benchmark_code(Selection.get_pandas_code(table1, "a", ">", "30"), repetitions))
#
#         input_project = t1_name, "a"
#         umbra_times[2].append(umbra.benchmark_query_code(Projection.get_sql_code(*input_project), repetitions))
#         postgres_times[2].append(postgres.benchmark_query_code(Projection.get_sql_code(*input_project), repetitions))
#         pandas_times[2].append(pandas.benchmark_code(Projection.get_pandas_code(table1, "a"), repetitions))
#         # in the end we have 3 lists == [[*joins*][*selections*][*projections*]]
#
#     names = ["Umbra", "Postgresql", "Pandas"]
#     for i, title in zip(range(3), ["Join", "Select", "Project"]):
#         table = [umbra_times[i], postgres_times[i], pandas_times[i]]
#         plot = plot_compare(f"{title} performance comparison:", REPETITIONS, all_y=table, all_y_names=names, save=True)
#         plot.waitforbuttonpress()


if __name__ == "__main__":
    # benchmark_of_basic_operations_simple_fake_data()
    benchmark_of_basic_operations_healthcare(r"/home/luca/Documents/Bachelorarbeit/umbra-students")
    # import pandas
    # import time
    # t0 = time.time()
    # df = pandas.read_csv(his, na_val="?")
    # result = df[["smoker"]]
    # t1 = time.time()
    # TODO: Assure everything was done correctly!!!!
    print()
