import matplotlib.pyplot as plt
import pathlib

import seaborn
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
import matplotlib
import seaborn as sns
import numpy as np
import datetime

ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent
PATH_TO_UMBRA = r"/home/luca/Documents/Bachelorarbeit/Umbra/umbra-students"
PATH_TO_QUERIES = str(pathlib.Path(ROOT_DIR) / r"related_SQL/healthcare_queries/queries")
PLOT_DIR = str(pathlib.Path(ROOT_DIR, "benchmarks/all_plots"))
REPETITIONS = [(10 ** i) for i in range(2, 5, 1)]
REPETITIONS_EXT = REPETITIONS  # + [pow(10, 6) * i for i in range(2, 10, 1)]

COLOR_SET = ["#B93313", "#188DD3", "#64B351", "#7F6AA7", "#D6BD62", "#BBDDAA"]
rc = {
    'axes.axisbelow': False,
    'axes.edgecolor': 'lightgrey',
    'axes.labelcolor': 'dimgrey',
    'lines.solid_capstyle': 'round',
    'patch.force_edgecolor': True,
    'text.color': 'dimgrey',
    'xtick.color': 'dimgrey',
    'xtick.direction': 'out',
    'xtick.top': False,
    'ytick.color': 'dimgrey',
    'ytick.direction': 'out',
    'ytick.right': False,
    'figure.figsize': (4., 4.)}

seaborn.set_theme(context="paper", font='Franklin Gothic Book', font_scale=1.2, style="whitegrid", palette=COLOR_SET,
                  rc=rc)


def plot_compare(title, x, all_y, all_y_names, colors=COLOR_SET, x_label="dataset size (rows)", y_label="runtime (ms)",
                 save=True):
    """
    Based on: mlinspect/experiments/performance/performance_benchmarks.ipynb
    Args:
        title (str): Title of the plot
        x (list): List of values for the x-axis
        all_y (list): List of lists containing the values for the y-axis
        all_y_names (list): List of strings for the names of the plots - same order as the list all_y
        colors (list or None): optional colors for the graphs passed in all_y
        x_label (str)
        y_label (str):
    """
    figure, axis = plt.subplots()

    axis.set_yscale('log')  # sets the scale to be logarithmic with powers of 10

    for j, (y, y_name) in enumerate(zip(all_y, all_y_names)):
        axis.plot(y, marker='8', color=colors[j], markersize=4, label=y_name, linewidth=2)
    # for marker type see: https://matplotlib.org/stable/api/markers_api.html#module-matplotlib.markers
    # plot function: https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.plot.html#matplotlib.axes.Axes.plot
    # first arguments set y_value range , x is set to 0..N-1 for now

    plt.legend(loc="upper left")
    axis.set_xticks(range(0, len(x)))
    axis.set_xticklabels(x)
    axis.set_axisbelow(True)

    # get the current labels
    labels = [item.get_text() for item in axis.get_xticklabels()]

    # Beat them in form and add back
    # see: https://stackoverflow.com/questions/36480077/python-plot-how-to-denote-ticks-on-the-axes-as-powers
    def get_exp(label):
        return int(np.log10(int(label)))

    # axis.set_xticklabels(
    #     [str(f"${int(int(label) / 10 ** get_exp(label))} \\times 10^{get_exp(label)}$") for label in labels])
    axis.set_xticklabels(
        [str(f"$10^{get_exp(label)}$") for label in labels])

    axis.set_facecolor('white')
    axis.axis('equal')
    axis.set(xlabel=x_label, ylabel=y_label)
    axis.grid(True, color='lightgrey')

    if save:
        figure.savefig(str(pathlib.Path(ROOT_DIR) / f"./benchmarks/all_plots/{title}_{datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S_')}.png"),
                       bbox_inches='tight', dpi=800)

    return plt


def plot_compare_bar_3d(title, x_list, y_list, z_list, x_label="engine", y_label="dataset size",
                        z_label="avg. time in ms"):
    """
    TODO
    With: https://stackoverflow.com/questions/31810776/formatting-a-3d-bar-plot-in-matplot-lib
    """
    # data = np.array([
    #     [0, 1, 0, 2, 0],
    #     [0, 3, 0, 2, 0],
    #     [6, 1, 1, 7, 0],
    #     [0, 5, 0, 2, 9],
    #     [0, 1, 0, 4, 0],
    #     [9, 1, 3, 4, 2],
    #     [0, 0, 2, 1, 3],
    # ])

    # x_list = ['a', 'b', 'c', 'd', 'e']
    # y_list = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

    fig = plt.figure()
    ax = Axes3D(fig)

    lx = len(x_list)  # Work out matrix dimensions
    ly = len(y_list)

    # Set up a mesh of positions: -> for list of lists put inner lists together:
    xpos = []
    i = 0
    for z_group in z_list:
        i += 1
        for _ in z_group:
            i += 0.5
            xpos.append(i)

    xpos = np.array(xpos)
    ypos = np.arange(0, ly, 1)
    xpos, ypos = np.meshgrid(xpos + 0.25, ypos + 0.25)

    xpos = xpos.flatten()  # Convert positions to 1D array
    ypos = ypos.flatten()
    zpos = np.zeros_like(np.array(z_list).flatten())

    dx = 0.5 * np.ones_like(zpos)
    dy = dx.copy()
    dz = np.array(z_list).flatten()

    cs = ['r', 'g', 'b'] * lx

    ax.bar3d(xpos, ypos, zpos, dx, dy, dz, color=cs, edgecolor='black')

    # sh()
    ax.w_xaxis.set_ticklabels(x_list)
    ax.w_yaxis.set_ticklabels(y_list)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_zlabel(z_label)

    plt.show()


def plot_compare_bar_2d(title, x, all_y, all_y_names, colors=None, x_label="dataset size", y_label="avg. time in ms"):
    """
    TODO
    Based on: mlinspect/experiments/performance/performance_benchmarks.ipynb
    Args:
        title (str): Title of the plot
        x (list): List of values for the x-axis
        all_y (list): List of lists containing the values for the y-axis
        all_y_names (list): List of strings for the names of the plots - same order as the list all_y
        colors (list or None): optional colors for the graphs passed in all_y
        x_label (str)
        y_label (str):
    """
    figure, axis = plt.subplots()

    axis.set_yscale('log')  # sets the scale to be logarithmic with powers of 10

    for j, (y, y_name) in enumerate(zip(all_y, all_y_names)):
        axis.plot(y, marker='8', color=(None if colors is None else colors[j]), markersize=3, label=y_name)
    # for marker type see: https://matplotlib.org/stable/api/markers_api.html#module-matplotlib.markers
    # plot function: https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.plot.html#matplotlib.axes.Axes.plot
    # first arguments set y_value range , x is set to 0..N-1 for now

    plt.legend(loc="upper left")
    axis.set_xticks(range(0, len(x)))
    axis.set_xticklabels(x)
    # set values regarding x - tick stand for 'small vertical lines' on the x-Axis

    axis.set_facecolor('white')
    axis.axis('equal')
    axis.set(xlabel=x_label, ylabel=y_label)
    axis.grid(True, color='lightgrey')

    figure.savefig("./all_plots" + title + ".png", bbox_inches='tight', dpi=800)

    plt.show()

    return

def get_healthcare_csv_paths():
    files = []
    for i in REPETITIONS:
        path_to_csv_his = ROOT_DIR / f"data_generation/generated_csv/healthcare_histories_generated_{i}.csv"
        path_to_csv_pat = ROOT_DIR / f"data_generation/generated_csv/healthcare_patients_generated_{i}.csv"
        files.append((path_to_csv_his, path_to_csv_pat))
    return files


if __name__ == "__main__":
    print()
