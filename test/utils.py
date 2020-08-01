"""
Some util functions used in other tests
"""
import os
import ast
import networkx

from mlinspect.instrumentation.dag_vertex import DagVertex
from mlinspect.instrumentation.wir_extractor import WirExtractor
from mlinspect.utils import get_project_root

FILE_PY = os.path.join(str(get_project_root()), "test", "pipelines", "adult_easy.py")
FILE_NB = os.path.join(str(get_project_root()), "test", "pipelines", "adult_easy.ipynb")


def get_expected_dag_adult_easy_py():
    """
    Get the expected DAG for the adult_easy pipeline
    """
    # pylint: disable=too-many-locals
    expected_graph = networkx.DiGraph()

    expected_data_source = DagVertex(18, "Data Source", 12, 11, ('pandas.io.parsers', 'read_csv'), "adult_train.csv")
    expected_graph.add_node(expected_data_source)

    expected_select = DagVertex(20, "Selection", 14, 7, ('pandas.core.frame', 'dropna'), "dropna")
    expected_graph.add_edge(expected_data_source, expected_select)

    expected_train_data = DagVertex(56, "Train Data", 28, 0, ('sklearn.pipeline', 'fit', 'Train Data'))
    expected_graph.add_edge(expected_select, expected_train_data)

    expected_pipeline_project_one = DagVertex(34, "Projection", 19, 75, ('sklearn.compose._column_transformer',
                                                                         'ColumnTransformer', 'Projection'),
                                              "to ['education']")
    expected_graph.add_edge(expected_train_data, expected_pipeline_project_one)
    expected_pipeline_project_two = DagVertex(35, "Projection", 19, 88, ('sklearn.compose._column_transformer',
                                                                         'ColumnTransformer', 'Projection'),
                                              "to ['workclass']")
    expected_graph.add_edge(expected_train_data, expected_pipeline_project_two)
    expected_pipeline_project_three = DagVertex(40, "Projection", 20, 49, ('sklearn.compose._column_transformer',
                                                                           'ColumnTransformer', 'Projection'),
                                                "to ['age']")
    expected_graph.add_edge(expected_train_data, expected_pipeline_project_three)
    expected_pipeline_project_four = DagVertex(41, "Projection", 20, 56, ('sklearn.compose._column_transformer',
                                                                          'ColumnTransformer', 'Projection'),
                                               "to ['hours-per-week']")
    expected_graph.add_edge(expected_train_data, expected_pipeline_project_four)

    expected_pipeline_transformer_one = DagVertex(34, "Transformer", 19, 20, ('sklearn.preprocessing._encoders',
                                                                              'OneHotEncoder', 'Pipeline'),
                                                  "Categorical Encoder (OneHotEncoder)")
    expected_graph.add_edge(expected_pipeline_project_one, expected_pipeline_transformer_one)
    expected_pipeline_transformer_two = DagVertex(35, "Transformer", 19, 20, ('sklearn.preprocessing._encoders',
                                                                              'OneHotEncoder', 'Pipeline'),
                                                  "Categorical Encoder (OneHotEncoder)")
    expected_graph.add_edge(expected_pipeline_project_two, expected_pipeline_transformer_two)
    expected_pipeline_transformer_three = DagVertex(40, "Transformer", 20, 16, ('sklearn.preprocessing._data',
                                                                                'StandardScaler', 'Pipeline'),
                                                    "Numerical Encoder (StandardScaler)")
    expected_graph.add_edge(expected_pipeline_project_three, expected_pipeline_transformer_three)
    expected_pipeline_transformer_four = DagVertex(41, "Transformer", 20, 16, ('sklearn.preprocessing._data',
                                                                               'StandardScaler', 'Pipeline'),
                                                   "Numerical Encoder (StandardScaler)")
    expected_graph.add_edge(expected_pipeline_project_four, expected_pipeline_transformer_four)

    expected_pipeline_concatenation = DagVertex(46, "Concatenation", 18, 25, ('sklearn.compose._column_transformer',
                                                                              'ColumnTransformer', 'Concatenation'))
    expected_graph.add_edge(expected_pipeline_transformer_one, expected_pipeline_concatenation)
    expected_graph.add_edge(expected_pipeline_transformer_two, expected_pipeline_concatenation)
    expected_graph.add_edge(expected_pipeline_transformer_three, expected_pipeline_concatenation)
    expected_graph.add_edge(expected_pipeline_transformer_four, expected_pipeline_concatenation)

    expected_estimator = DagVertex(51, "Estimator", 26, 19, ('sklearn.tree._classes', 'DecisionTreeClassifier',
                                                             'Pipeline'),
                                   "Decision Tree")
    expected_graph.add_edge(expected_pipeline_concatenation, expected_estimator)

    expected_pipeline_fit = DagVertex(56, "Fit Transformers and Estimators", 28, 0, ('sklearn.pipeline', 'fit',
                                                                                     'Pipeline'))
    expected_graph.add_edge(expected_estimator, expected_pipeline_fit)

    expected_project = DagVertex(23, "Projection", 16, 38, ('pandas.core.frame', '__getitem__'),
                                 "to ['income-per-year']")
    expected_graph.add_edge(expected_select, expected_project)

    expected_project_modify = DagVertex(28, "Projection (Modify)", 16, 9,
                                        ('sklearn.preprocessing._label', 'label_binarize'),
                                        "label_binarize, classes: ['>50K', '<=50K']")
    expected_graph.add_edge(expected_project, expected_project_modify)

    expected_train_labels = DagVertex(56, "Train Labels", 28, 0, ('sklearn.pipeline', 'fit', 'Train Labels'))
    expected_graph.add_edge(expected_project_modify, expected_train_labels)
    expected_graph.add_edge(expected_train_labels, expected_pipeline_fit)

    return expected_graph


def get_expected_dag_adult_easy_ipynb():
    """
    Get the expected DAG for the adult_easy pipeline
    """
    # pylint: disable=too-many-locals
    expected_graph = networkx.DiGraph()

    expected_data_source = DagVertex(18, "Data Source", 18, 11, ('pandas.io.parsers', 'read_csv'), "adult_train.csv")
    expected_graph.add_node(expected_data_source)

    expected_select = DagVertex(20, "Selection", 20, 7, ('pandas.core.frame', 'dropna'), "dropna")
    expected_graph.add_edge(expected_data_source, expected_select)

    expected_train_data = DagVertex(56, "Train Data", 34, 0, ('sklearn.pipeline', 'fit', 'Train Data'))
    expected_graph.add_edge(expected_select, expected_train_data)

    expected_pipeline_project_one = DagVertex(34, "Projection", 25, 75, ('sklearn.compose._column_transformer',
                                                                         'ColumnTransformer', 'Projection'),
                                              "to ['education']")
    expected_graph.add_edge(expected_train_data, expected_pipeline_project_one)
    expected_pipeline_project_two = DagVertex(35, "Projection", 25, 88, ('sklearn.compose._column_transformer',
                                                                         'ColumnTransformer', 'Projection'),
                                              "to ['workclass']")
    expected_graph.add_edge(expected_train_data, expected_pipeline_project_two)
    expected_pipeline_project_three = DagVertex(40, "Projection", 26, 49, ('sklearn.compose._column_transformer',
                                                                           'ColumnTransformer', 'Projection'),
                                                "to ['age']")
    expected_graph.add_edge(expected_train_data, expected_pipeline_project_three)
    expected_pipeline_project_four = DagVertex(41, "Projection", 26, 56, ('sklearn.compose._column_transformer',
                                                                          'ColumnTransformer', 'Projection'),
                                               "to ['hours-per-week']")
    expected_graph.add_edge(expected_train_data, expected_pipeline_project_four)

    expected_pipeline_transformer_one = DagVertex(34, "Transformer", 25, 20, ('sklearn.preprocessing._encoders',
                                                                              'OneHotEncoder', 'Pipeline'),
                                                  "Categorical Encoder (OneHotEncoder)")
    expected_graph.add_edge(expected_pipeline_project_one, expected_pipeline_transformer_one)
    expected_pipeline_transformer_two = DagVertex(35, "Transformer", 25, 20, ('sklearn.preprocessing._encoders',
                                                                              'OneHotEncoder', 'Pipeline'),
                                                  "Categorical Encoder (OneHotEncoder)")
    expected_graph.add_edge(expected_pipeline_project_two, expected_pipeline_transformer_two)
    expected_pipeline_transformer_three = DagVertex(40, "Transformer", 26, 16, ('sklearn.preprocessing._data',
                                                                                'StandardScaler', 'Pipeline'),
                                                    "Numerical Encoder (StandardScaler)")
    expected_graph.add_edge(expected_pipeline_project_three, expected_pipeline_transformer_three)
    expected_pipeline_transformer_four = DagVertex(41, "Transformer", 26, 16, ('sklearn.preprocessing._data',
                                                                               'StandardScaler', 'Pipeline'),
                                                   "Numerical Encoder (StandardScaler)")
    expected_graph.add_edge(expected_pipeline_project_four, expected_pipeline_transformer_four)

    expected_pipeline_concatenation = DagVertex(46, "Concatenation", 24, 25, ('sklearn.compose._column_transformer',
                                                                              'ColumnTransformer', 'Concatenation'))
    expected_graph.add_edge(expected_pipeline_transformer_one, expected_pipeline_concatenation)
    expected_graph.add_edge(expected_pipeline_transformer_two, expected_pipeline_concatenation)
    expected_graph.add_edge(expected_pipeline_transformer_three, expected_pipeline_concatenation)
    expected_graph.add_edge(expected_pipeline_transformer_four, expected_pipeline_concatenation)

    expected_estimator = DagVertex(51, "Estimator", 32, 19, ('sklearn.tree._classes', 'DecisionTreeClassifier',
                                                             'Pipeline'),
                                   "Decision Tree")
    expected_graph.add_edge(expected_pipeline_concatenation, expected_estimator)

    expected_pipeline_fit = DagVertex(56, "Fit Transformers and Estimators", 34, 0, ('sklearn.pipeline', 'fit',
                                                                                     'Pipeline'))
    expected_graph.add_edge(expected_estimator, expected_pipeline_fit)

    expected_project = DagVertex(23, "Projection", 22, 38, ('pandas.core.frame', '__getitem__'),
                                 "to ['income-per-year']")
    expected_graph.add_edge(expected_select, expected_project)

    expected_project_modify = DagVertex(28, "Projection (Modify)", 22, 9,
                                        ('sklearn.preprocessing._label', 'label_binarize'),
                                        "label_binarize, classes: ['>50K', '<=50K']")
    expected_graph.add_edge(expected_project, expected_project_modify)

    expected_train_labels = DagVertex(56, "Train Labels", 34, 0, ('sklearn.pipeline', 'fit', 'Train Labels'))
    expected_graph.add_edge(expected_project_modify, expected_train_labels)
    expected_graph.add_edge(expected_train_labels, expected_pipeline_fit)

    return expected_graph


def get_module_info():
    """
    Get the module info for the adult_easy pipeline
    """
    module_info = {(10, 0): ('builtins', 'print'),
                   (11, 13): ('posixpath', 'join'),
                   (11, 26): ('builtins', 'str'),
                   (11, 30): ('mlinspect.utils', 'get_project_root'),
                   (12, 11): ('pandas.io.parsers', 'read_csv'),
                   (14, 7): ('pandas.core.frame', 'dropna'),
                   (16, 9): ('sklearn.preprocessing._label', 'label_binarize'),
                   (16, 38): ('pandas.core.frame', '__getitem__'),
                   (18, 25): ('sklearn.compose._column_transformer', 'ColumnTransformer'),
                   (19, 20): ('sklearn.preprocessing._encoders', 'OneHotEncoder'),
                   (20, 16): ('sklearn.preprocessing._data', 'StandardScaler'),
                   (24, 18): ('sklearn.pipeline', 'Pipeline'),
                   (26, 19): ('sklearn.tree._classes', 'DecisionTreeClassifier'),
                   (28, 0): ('sklearn.pipeline', 'fit'),
                   (31, 0): ('builtins', 'print')}

    return module_info


def get_call_description_info():
    """
    Get the module info for the adult_easy pipeline
    """
    call_description_info = {
        (12, 11): 'adult_train.csv',
        (14, 7): 'dropna',
        (16, 38): 'to [\'income-per-year\']',
        (16, 9): 'label_binarize, classes: [\'>50K\', \'<=50K\']',
        (19, 20): 'Categorical Encoder (OneHotEncoder)',
        (20, 16): 'Numerical Encoder (StandardScaler)',
        (26, 19): 'Decision Tree'
    }

    return call_description_info


def get_adult_easy_py_ast():
    """
    Get the parsed AST for the adult_easy pipeline
    """
    with open(FILE_PY) as file:
        test_code = file.read()

        test_ast = ast.parse(test_code)
    return test_ast


def get_test_wir():
    """
    Get the extracted WIR for the adult_easy pipeline with runtime info
    """
    test_ast = get_adult_easy_py_ast()
    extractor = WirExtractor(test_ast)
    extractor.extract_wir()
    wir = extractor.add_runtime_info(get_module_info(), get_call_description_info())

    return wir
