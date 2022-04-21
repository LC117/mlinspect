import warnings
from benchmarks.benchmark_utility import plot_compare, REPETITIONS, get_healthcare_csv_paths
from related_SQL.umbra_utility import UmbraSession
from related_SQL.postgreql_utility import PostgresqlSession
from related_SQL.pandas_utility import PandasSession
from inspect import cleandoc

# os.environ['OPENBLAS_NUM_THREADS'] = '1'
# os.environ['MKL_NUM_THREADS'] = '1'

warnings.filterwarnings("ignore")


def _get_pandas_query_only_def_sql(path_to_patient_csv, path_to_history_csv, na_values="?"):
    pandas_code = cleandoc(f"""    
    COUNTIES_OF_INTEREST = ['county2', 'county3']
    patients = pd.read_csv(r"{path_to_patient_csv}", na_values="{na_values}")
    histories = pd.read_csv(r"{path_to_history_csv}", na_values="{na_values}")
    # combine input data into a single table
    data = patients.merge(histories, on=['ssn'])
    # compute mean complications per age group, append as column
    complications = data.groupby('age_group').agg(mean_complications=('complications', 'mean'))
    data = data.merge(complications, on=['age_group'])
    # target variable: people with a high number of complications
    data['label'] = data['complications'] > 1.2 * data['mean_complications']
    # project data to a subset of attributes
    data = data[['smoker', 'last_name', 'county', 'num_children', 'race', 'income', 'label']]
    # filter data
    data = data[data['county'].isin(COUNTIES_OF_INTEREST)]
    # NOW COMES THE REST: IMPUTE, W2V, ETC. (BUT, NOT DEFAULT SQL)
    """)
    return pandas_code


def main():
    queries = [
        "related_SQL/healthcare_queries/queries/only_default_sql.sql",
        "related_SQL/healthcare_queries/queries/only_default_sql_umbra.sql"
    ]
    """
    Based on mlinspect benchmarks.
    """
    t1_name = "histories"
    t2_name = "patients"

    files = get_healthcare_csv_paths()

    umbra_times = []
    postgres_times = []
    pandas_times = []

    for i, (table1, table2) in enumerate(files):
        umbra = UmbraSession()
        postgres = PostgresqlSession()
        pandas = PandasSession()

        umbra.add_csv_to_db(t1_name, table1)
        umbra.add_csv_to_db(t2_name, table2)
        postgres.add_csv_to_db(t1_name, table1)
        postgres.add_csv_to_db(t2_name, table2)

        print(f"ITERATION: {i} - for table size of: {10 ** (i + 2)}")
        repetitions = 5

        input_join = t1_name, t2_name, "ssn"
        umbra_times.append(umbra.benchmark_query_file(queries[1], repetitions))
        postgres_times.append(postgres.benchmark_query_file(queries[0], repetitions))
        pandas_times.append(pandas.benchmark_code(
            _get_pandas_query_only_def_sql(path_to_history_csv=table1, path_to_patient_csv=table2),
            setup_code="import pandas as pd", repetitions=repetitions))

    names = ["Umbra", "Postgresql", "Pandas"]
    title = "healthcare_benchmark_compare_only_def_sql"
    table = [umbra_times, postgres_times, pandas_times]
    plot = plot_compare(f"{title} performance comparison:", REPETITIONS, all_y=table, all_y_names=names, save=True)
    plot.waitforbuttonpress()


if __name__ == "__main__":
    main()
