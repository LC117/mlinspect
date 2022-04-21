from pathlib import Path
from related_SQL.postgreql_utility import PostgresqlSession
from related_SQL.pandas_utility import PandasSession
from inspect import cleandoc
from benchmarks.benchmark_utility import plot_compare
import pandas as pd

here = Path.cwd()
sess_p = PostgresqlSession()
# conn = sess_p.get_connection()

sess_pandas = PandasSession()
pandas_code = cleandoc(f"""
data = pd.read_sql('SELECT * FROM data', conn)
data['label'] = data['b'] > 1.2 * data['c']
""")

results_f = []
results_s = []
results_si = []
results_p = []

x = [(10 ** i) for i in range(2, 8, 1)]
for i in x:
    sql_code = cleandoc(f"""
    DROP TABLE IF EXISTS data;
    
    create table data (data_f int, b int, c int, index_ SERIAL UNIQUE);
    
    insert into data (data_f, b, c) 
    (with b as (
        select id, id, id
        from generate_series(1, {i}) as id
        order by random()) select * from b);
    """)
    sess_p.execute_query_code(sql_code)

    results_f.append(sess_p.benchmark_query_file(here / "fast.sql", 1))
    # results_s.append(sess_p.benchmark_query_file(here / "slow.sql", 3))
    results_si.append(sess_p.benchmark_query_file(here / "slow_index.sql", 1))
    # results_p.append(sess_pandas.benchmark_code(pandas_code,
    #                                             setup_code="import pandas as pd \n" +
    #                                                        "from related_SQL.postgreql_utility import ENGINE \n" +
    #                                                        "conn = ENGINE.connect()",
    #                                             repetitions=3))
plot_compare(title="naive_vs_simple_vs_index",
             # Performance comparison naive, naive with index and simple both in Postgresql
             x=x, all_y=[results_f, results_si],
             all_y_names=["ideal SQL", "with index column SQL"])
