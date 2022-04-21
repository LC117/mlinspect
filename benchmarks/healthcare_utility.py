from inspect import cleandoc
from benchmarks.benchmark_utility import ROOT_DIR

REPETITIONS = [(10 ** i) for i in range(2, 5, 1)]


def get_healthcare_csv_paths():
    files = []
    for i in REPETITIONS:
        path_to_csv_his = ROOT_DIR / f"data_generation/generated_csv/healthcare_histories_generated_{i}.csv"
        path_to_csv_pat = ROOT_DIR / f"data_generation/generated_csv/healthcare_patients_generated_{i}.csv"
        files.append((path_to_csv_his, path_to_csv_pat))
    return files


def get_healthcare_pipe_code_orig(path_histories, path_patients):
    setup_code = cleandoc("""
        import os
        import pandas as pd
        from mlinspect.utils import get_project_root
        """)

    test_code = cleandoc(f"""
        COUNTIES_OF_INTEREST = ['county2', 'county3']

        patients = pd.read_csv('{path_patients}', na_values='?')
        histories = pd.read_csv('{path_histories}', na_values='?')

        data = patients.merge(histories, on=['ssn'])
        complications = data.groupby('age_group') \
            .agg(mean_complications=('complications', 'mean'))
        data = data.merge(complications, on=['age_group'])
        data['label'] = data['complications'] > 1.2 * data['mean_complications']
        data = data[['smoker', 'last_name', 'county', 'num_children', 'race', 'income', 'label']]
        data = data[data['county'].isin(COUNTIES_OF_INTEREST)]
        """)
    return setup_code, test_code
