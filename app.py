from ast import literal_eval
import json
import numpy as np
import os
import time

import dash
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html

from inspect import cleandoc

import networkx as nx
from networkx.drawing.nx_agraph import to_agraph

import plotly.graph_objects as go
import pygraphviz as pgv

from demo.feature_overview.no_missing_embeddings import NoMissingEmbeddings
from mlinspect import PipelineInspector
from mlinspect.checks import NoBiasIntroducedFor, NoIllegalFeatures
from mlinspect.inspections import HistogramForColumns, RowLineage, MaterializeFirstOutputRows
from mlinspect.visualisation import save_fig_to_path


# Initialize Dash app with external stylesheets
app = dash.Dash(__name__, external_stylesheets=[
    # Dash CSS
    "https://codepen.io/chriddyp/pen/bWLwgP.css",
    # Loading screen CSS
    "https://codepen.io/chriddyp/pen/brPBPO.css",
    # Bootstrap theme CSS
    dbc.themes.BOOTSTRAP,  # pro: CSS classes; con: tiny font size
    # dbc.themes.GRID,  # pro: grid layouts, large enough font size; con: no other dbc elements or CSS classes
])
app.config.suppress_callback_exceptions = True


# Create HTML layout
CODE_FONT = {"font-family": "'Courier New', monospace"}
app.title = "mlinspect"
with open("example_pipelines/healthcare/healthcare.py") as f:
    default_pipeline = f.read()
sensitive_columns = ["age_group", "race"]
inspection_switcher = {
    "HistogramForColumns": lambda: HistogramForColumns(sensitive_columns),
    "RowLineage": lambda: RowLineage(5),
    "MaterializeFirstOutputRows": lambda: MaterializeFirstOutputRows(5),
}
check_switcher = {
    "NoBiasIntroducedFor": lambda: NoBiasIntroducedFor(sensitive_columns),
    "NoIllegalFeatures": NoIllegalFeatures,
    "NoMissingEmbeddings": NoMissingEmbeddings,
}
app.layout = dbc.Container([
    # Header and description
    html.H1("mlinspect", style=CODE_FONT),
    html.P("Inspect ML Pipelines in Python in the form of a DAG."),

    dcc.Tabs([
        # Inspection definition tab
        dcc.Tab([
            dbc.Form([
                dbc.Row([
                    dbc.Col([
                        dbc.FormGroup([
                            # Pipeline definition
                            dbc.Label("Pipeline definition:", html_for="pipeline"),
                            dbc.Textarea(
                                id="pipeline",
                                className="mb-3",
                                style={"width": "700px", "height": "500px", **CODE_FONT},
                                value=default_pipeline,
                            ),
                        ]),
                    ]),
                    dbc.Col([
                        dbc.FormGroup([
                            # Add inspections
                            dbc.Label("Add required inspections:", html_for="inspections"),
                            dbc.Checklist(
                                id="inspections",
                                options=[
                                    {"label": "Histogram For Columns", "value": "HistogramForColumns"},
                                    {"label": "Row Lineage", "value": "RowLineage"},
                                    {"label": "Materialize First Output Rows", "value": "MaterializeFirstOutputRows"},
                                ],
                                switch=True,
                                value=[],
                            ),
                        ]),
                        dbc.FormGroup([
                            # Add checks
                            dbc.Label("Add checks:", html_for="checks"),
                            dbc.Checklist(
                                id="checks",
                                options=[
                                    {"label": "No Bias Introduced For", "value": "NoBiasIntroducedFor"},
                                    {"label": "No Illegal Features", "value": "NoIllegalFeatures"},
                                    {"label": "No Missing Embeddings", "value": "NoMissingEmbeddings"},
                                ],
                                switch=True,
                                value=[],
                            ),
                        ]),
                        # Execute inspection
                        dbc.Button("Inspect pipeline", id="execute", color="primary", size="lg", className="mr-1"),
                    ]),
                ]),
            ], style={"margin": "auto", "padding": "20px"}),
        ], label="INSPECTION DEFINITION", value="definition-tab"),

        # Inspection results tab
        dcc.Tab([
            dbc.Row([
                dbc.Col([
                    # Display code
                    html.P(
                        id="pipeline-output",
                        className="mb-3",
                        style={**CODE_FONT},
                        children=default_pipeline,
                    ),
                ], width=6),
                dbc.Col([
                    # Display DAG
                    # dbc.Label("Extracted DAG:", html_for="dag"),
                    dcc.Graph(id="dag", figure=go.Figure(
                        # layout_width=650,
                        layout_height=650,
                        layout_showlegend=False,
                        layout_xaxis={'visible': False},
                        layout_yaxis={'visible': False},
                    )),
                ], width=6),
            ], style={"margin": "auto", "padding": "20px"}),
        ], label="INSPECTION RESULTS", value="results-tab"),
    ], id="tabs"),

    # Body
    # dbc.Row([  # Grid only works with dbc.themes in external_stylesheets.
    #     dbc.Col([
    #         dbc.Form([
    #             dbc.FormGroup([
    #                 # Pipeline definition
    #                 dbc.Label("Pipeline definition:", html_for="pipeline"),
    #                 dbc.Textarea(id="pipeline", className="mb-3", style={"width": "450px", "height": "500px", **CODE_FONT},
    #                              value=default_pipeline),
    #             ]),
    #             dbc.FormGroup([
    #                 # Add inspections
    #                 dbc.Label("Add required inspections:", html_for="inspections"),
    #                 dbc.Checklist(
    #                     id="inspections",
    #                     options=[
    #                         {"label": "Histogram For Columns", "value": "HistogramForColumns"},
    #                         {"label": "Row Lineage", "value": "RowLineage"},
    #                         {"label": "Materialize First Output Rows", "value": "MaterializeFirstOutputRows"},
    #                     ],
    #                     switch=True,
    #                     value=[],
    #                 ),
    #             ]),
    #             dbc.FormGroup([
    #                 # Add checks
    #                 dbc.Label("Add checks:", html_for="checks"),
    #                 dbc.Checklist(
    #                     id="checks",
    #                     options=[
    #                         {"label": "No Bias Introduced For", "value": "NoBiasIntroducedFor"},
    #                         {"label": "No Illegal Features", "value": "NoIllegalFeatures"},
    #                         {"label": "No Missing Embeddings", "value": "NoMissingEmbeddings"},
    #                     ],
    #                     switch=True,
    #                     value=[],
    #                 ),
    #             ]),
    #         ]),
    #         # Execute inspection
    #         dbc.Button("Inspect pipeline", id="execute", color="primary", size="lg", className="mr-1"),
    #     ], width=6),
    #     dbc.Col([
    #         # Display DAG
    #         dbc.Label("Extracted DAG:", html_for="dag"),
    #         dcc.Graph(id="dag", figure=go.Figure(
    #             layout_width=650,
    #             layout_height=650,
    #             layout_showlegend=False,
    #             layout_xaxis={'visible': False},
    #             layout_yaxis={'visible': False},
    #         )),
    #     ], width=6),
    # ]),
])


# Flask server (for gunicorn)
server = app.server


@app.callback(
    Output("pipeline-output", "children"),
    Input("execute", "n_clicks"),
    state=[
        State("pipeline", "value")
    ])
def update_pipeline_output(n_clicks, pipeline):
    if n_clicks is None:
        return dash.no_update

    # TODO: Add formatting, e.g. red text color for problem lines

    return pipeline


@app.callback([
        Output("dag", "figure"),
        Output("tabs", "value"),
    ],
    [
        Input("execute", "n_clicks"),
    ],
    state=[
        State("pipeline", "value"),
        State("checks", "value"),
        State("inspections", "value"),
    ])
def update_figure(n_clicks, pipeline, checks, inspections):
    """Dash callback function to show extracted DAG of ML pipeline."""
    if n_clicks is None:
        return dash.no_update

    extracted_dag, inspection_results, check_results = extract_dag(pipeline, checks, inspections)

    # === DAG figure ===
    fig = nx2go(extracted_dag)

    # print("fig attributes: type(fig.data[0]) =", type(fig.data[0]))
    # fig_dict = fig.to_dict()
    # --- write to file ---
    # with open('dag2.json', 'w') as f:
    #     json.dump(fig_dict, f, indent="\t", ensure_ascii=True)
    # --- read from file ---
    # with open('dag2.json', 'r') as f:
    #     fig_dict = json.load(f)

    # === Inspection results ===
    # TODO: Add another scatter graph to figure, with problem nodes in red
    fig = add_inspection_annotations(fig, extracted_dag, inspection_results)

    return fig, "results-tab"


def extract_dag(pipeline, checks=None, inspections=None):
    """Extract DAG the original way, i.e. by creating a PipelineInspectorBuilder."""
    start = time.time()
    builder = PipelineInspector.on_pipeline_from_string(pipeline)
    for inspection in inspections:
        builder = builder.add_required_inspection(inspection_switcher[inspection]())
    for check in checks:
        builder = builder.add_check(check_switcher[check]())
    inspector_result = builder.execute()
    print(f"Total time in seconds: {time.time() - start}")

    extracted_dag = inspector_result.dag
    inspection_results = inspector_result.inspection_to_annotations
    check_results = inspector_result.check_to_check_results
    return extracted_dag, inspection_results, check_results


def nx2png(extracted_dag):
    """Convert networkx.DiGraph to a pygraphviz.agraph.AGraph, save to file, and return filename.
    Also return boolean of whether HTML Image element is hidden."""
    filename = os.path.join(os.getcwd(), app.get_asset_url("image.png"))
    save_fig_to_path(extracted_dag, filename)
    return filename, False


def nx2agraph(extracted_dag):
    """Convert networkx.DiGraph to a pygraphviz.agraph.AGraph."""
    extracted_dag = nx.relabel_nodes(extracted_dag, lambda node: cleandoc("""
        {} (L{})
        {}
        """.format(node.operator_type.value, node.code_reference.lineno, node.description or "")))
    agraph = to_agraph(extracted_dag)
    agraph.layout('dot')
    return agraph


def get_new_node_label(node):
    """From mlinspect.visualisation._visualisation."""
    label = cleandoc("""
            {} (L{})
            {}
            """.format(node.operator_type.value, node.code_reference.lineno, node.description or ""))
    return label


def _get_pos(G):
    pos_dict = nx.nx_agraph.graphviz_layout(G, 'dot')
    # pos_json = {k.node_id: {'pos': v, 'node': k.to_dict()} for k, v in pos_dict.items()}
    # with open('pos_dict_with_checks.json', 'w') as f:
    #     json.dump(pos_json, f, indent="\t", ensure_ascii=True)

    nodes = G.nodes()
    edges = G.edges()

    Xn = []
    Yn = []
    for node in nodes:
        x, y = pos_dict[node]
        Xn += [x]
        Yn += [y]

    Xe = []
    Ye = []
    from addEdge import add_edge
    for edge0, edge1 in edges:
        # x0, y0 = pos_dict[edge0]
        # x1, y1 = pos_dict[edge1]
        # Xe += [x0, x1, None]
        # Ye += [y0, y1, None]
        Xe, Ye = add_edge(
            pos_dict[edge0],
            pos_dict[edge1],
            Xe,
            Ye,
            length_frac=0.8,
            arrow_pos='end',
            arrow_length=130,
            arrow_angle=5,
            # dot_size=15,
        )

    labels = []
    annotations = []
    for node, pos in pos_dict.items():
        labels += [get_new_node_label(node)]
        annotations += [{
            'x': pos[0],
            'y': pos[1],
            'text': node.operator_type.short_value,
            'showarrow': False,
        }]

    return Xn, Yn, Xe, Ye, labels, annotations


def nx2go(G):
    """
    Convert networkx.DiGraph to a plotly.graph_objects.Figure.

    Adapted from: https://chart-studio.plotly.com/~empet/14007/graphviz-networks-plotted-with-plotly/#/
    """
    Xn, Yn, Xe, Ye, labels, annotations = _get_pos(G)

    edges = go.Scatter(x=Xe, y=Ye, mode='lines', hoverinfo='none',
                       line={
                           'color': 'rgb(160,160,160)',
                           'width': 0.75,
                        })
    nodes = go.Scatter(x=Xn, y=Yn, mode='markers', name='', hoverinfo='text', text=labels,
                       marker={
                           'size': 15,
                           'color': '#85b6b6',
                           'line': {
                               'color': 'rgb(100,100,100)',
                               'width': 0.5,
                            },
                        })
    layout = go.Layout(
                title="Pipeline execution DAG",
                # font={'family': 'Balto'},
                font={'family': "'Courier New', monospace"},
                # width=650,
                height=650,
                showlegend=False,
                xaxis={'visible': False},
                yaxis={'visible': False},
                # margin={'t': 100},
                hovermode='closest',
    )
    layout.annotations = annotations

    fig = go.Figure(data=[edges, nodes], layout=layout)

    return fig


def add_inspection_annotations(figure, extracted_dag, inspection_results):
    first_rows_inspection_result = inspection_results[MaterializeFirstOutputRows(5)]

    relevant_nodes = [node for node in extracted_dag.nodes if node.description in {
        "Imputer (SimpleImputer), Column: 'county'", "Categorical Encoder (OneHotEncoder), Column: 'county'"}]

    # Create scatter plot of these nodes
    pos_dict = nx.nx_agraph.graphviz_layout(extracted_dag, 'dot')  # TODO: Reuse from before rather than rerunning
    Xn = []
    Yn = []
    for node in relevant_nodes:
        x, y = pos_dict[node]
        Xn += [x]
        Yn += [y]
    nodes = go.Scatter(x=Xn, y=Yn, mode='markers', #name='', hoverinfo='text', text=labels,
                       marker={
                           'size': 15,
                           'color': 'red',
                           'line': {
                               'color': 'red',
                               'width': 0.5,
                            },
                        })

    # Append scatter plot to figure
    # new_figure = go.Figure(data=figure.data + [nodes], layout=figure.layout)
    figure.add_trace(nodes)

    # for dag_node in relevant_nodes:
    #     if dag_node in first_rows_inspection_result and first_rows_inspection_result[dag_node] is not None:
    #         print("\n\033[1m{} ({})\033[0m\n{}\n{}".format(
    #             dag_node.operator_type, dag_node.description, dag_node.source_code, dag_node.code_reference))
    #         display(first_rows_inspection_result[dag_node])

    # return new_figure
    return figure


if __name__ == "__main__":
    # Disable TensorFlow warnings in logs
    # os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

    # Run Dash server
    app.run_server(host="0.0.0.0", debug=True)
