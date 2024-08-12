import argparse
import os
from typing import List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from utils import clean_data, read_number_of_nodes

VERSIONS = []
DYNAMIC_VERSION = True
VERSIONS_LABEL_MAPPING = {
    '0': 'SAN HDD',
    '1': 'user\npartitioning',
    '11': 'FCFS',
    '12': 'FCFS+SJF',
    '2': 'semi-smart\npartitioning',
    '21': 'hit-\naware',
    '25': 'hit &\ndata\n-aware',
    '29': 'hit &\ndata &\n load-aware',
    '3': 'smart\npartitioning',
    '4': 'time\naware',
    '41': 'time &\nfuture-aware',
    '42': 'time &\nfuture-aware\n(heuristic)',
}

NUMBER_OF_NODES_PARTITIONS = read_number_of_nodes()


def autolabel_percents_partition(rects, percents: List[int]):
    for i, rect in enumerate(rects):
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() * 0.5, height,
                f'{round(100 * percents[i])}%',
                ha='center', fontweight="bold", fontsize=15)


def add_labels(x, y):
    for i in range(len(x)):
        plt.text(i - 0.12, y[i] + 0.005, y[i], weight="bold", fontsize=10)


def draw(resource: str):
    x = []
    y = []
    color = []
    for index, version in enumerate(VERSIONS):
        x.append(VERSIONS_LABEL_MAPPING[version])
        y.append(eval(f'utilization_{version}')[0])
        if index % 2 == 0:
            color.append('darkblue')
        else:
            color.append('lightsteelblue')

    plt.bar(x, y, color=color)
    add_labels(x, y)
    plt.xticks(x, x, fontsize=10)
    plt.title(resource.replace('_', ' '), fontsize=13, weight="bold")
    plt.ylabel("Normalized Time", fontsize=12, weight="bold")
    plt.savefig(f'visualization/utilization/{INPUT_ADDRESS}_{target_resource}.jpg', bbox_inches='tight')


"""
this script illustrates number each partition for each of the user_partitioning
and smart_partitioning.
"""
if __name__ == '__main__':
    fake = pd.DataFrame  # todo nothing
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input")
    parser.add_argument("-r", "--resource")
    args = parser.parse_args()

    print(args.input)
    INPUT_ADDRESS = f'{args.input}'
    current_directory = os.getcwd()
    print(current_directory)
    directory_path = os.path.join(current_directory, INPUT_ADDRESS)
    files = [f for f in os.listdir(directory_path)]
    target_resource = args.resource
    if DYNAMIC_VERSION:
        VERSIONS = []
    # ===================================================
    for f in files:
        version = f.title().split('.')[0][1:]
        if DYNAMIC_VERSION:
            VERSIONS.append(version)
        exec(f'jobs_{version} = pd.read_csv(f"{directory_path}/{f}").sort_values("job_id")')
        clean_data(eval(f'jobs_{version}'))

    for version in VERSIONS:
        exec(f'total_period_{version} = jobs_{version}["finish_time"].max() - jobs_{version}["submission_time"].min()')
        exec(f'utilization_{version} = []')
        exec(f'jobs_{version}_{target_resource} = jobs_{version}[jobs_{version}["partition"] == "{target_resource}"]')
        exec(
            f'utilization = round(jobs_{version}_{target_resource}["execution_time"].sum() / ((total_period_{version} * {NUMBER_OF_NODES_PARTITIONS[target_resource]})), 2)')
        exec(f'utilization_{version}.append(utilization)')

    draw(resource=target_resource)
    # fig, ax = plt.subplots()
    # x = np.arange(1)
    # width = 0.09
    # colors = ['orange', 'red', 'green', 'blue', 'purple', 'gray', 'pink', 'khaki']
    # margin = 0
    # for version in VERSIONS:
    #     # print(eval(f'utilization_{version}'))
    #     position = x + width / len(VERSIONS)
    #     position += margin
    #     margin += 0.1
    #     rects = ax.bar(
    #         position,
    #         eval(f'utilization_{version}'),
    #         width,
    #         label=VERSIONS_LABEL_MAPPING[version],
    #         color=colors[VERSIONS.index(version)]
    #     )
    #     exec(f'rects_{version} = rects')
    #     exec(f'percent_{version} = np.array(utilization_{version}) / sum(utilization_{version})')
    #     print("*************88")
    #     print(version + ': ' + str(eval(f'utilization_{version}')))
    #     print("*************88")
    #
    #
    # for version in VERSIONS:
    #     autolabel_percents_partition(eval(f'rects_{version}'), eval(f'utilization_{version}'))
    #
    # ax.legend(bbox_to_anchor=(1.2, 1.2), loc='upper right', borderaxespad=0, fontsize=12)
    #
    # ax.yaxis.set_tick_params(labelsize=14)
    # ax.set_title('Utilization', fontsize=20)
    # ax.set_ylabel('percent', fontsize=20)
    # plt.xticks([])
    # plt.ylim(0, 1)
    # ax.set_xlabel(target_resource, fontsize=20)
    # plt.yticks(fontsize=15)
    #
    # fig.tight_layout()
    # # plt.show()
    # fig.set_size_inches(w=16, h=9)
    # plt.savefig(f'visualization/utilization/{INPUT_ADDRESS}_{target_resource}.jpg', bbox_inches='tight')
