import argparse
import os

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from utils import clean_data, read_number_of_nodes, select_cpu_intensive_jobs

PARAMETERS = {
    'e': 'execution_time',
    'w': 'waiting_time',
    't': 'turnaround_time',
    's': 'slowdown_time',
    'f': 'user_fairness',
    'd': 'finish_time'
}
RESOURCES = {
    'h': 'central_hdd',
    'c': 'cache_with_hdd',
    's': 'local_ssd',
    'a': None
}

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


def add_labels(x, y):
    for i in range(len(x)):
        plt.text(i - 0.12, y[i] + 0.5, y[i], weight="bold", fontsize=10)


def draw(output_filename: str, parameter: str, resource: str, data: dict, avg=True):
    for version in VERSIONS:
        exec(f'jobs_{version}_results = int(jobs_{version}_results_{"avg" if avg else "sum"}[0] * 1000) / 10')

    for version in VERSIONS:
        exec(f'diff_from_max_{version} = jobs_{VERSIONS[0]}_results - jobs_{version}_results')

    x = []
    y = []
    color = []
    for index, version in enumerate(VERSIONS):
        x.append(VERSIONS_LABEL_MAPPING[version])
        y.append(eval(f'jobs_{version}_results'))
        if index % 2 == 0:
            color.append('darkblue')
        else:
            color.append('lightsteelblue')
    plt.bar(x, y, color=color)
    add_labels(x, y)
    plt.xticks(x, x, fontsize=10)
    plt.title(parameter.replace('_', ' '), fontsize=13, weight="bold")
    plt.ylabel("Normalized Time", fontsize=12, weight="bold")
    print("-----------------------")
    print(f"visualization/{OUTPUT_ADDRESS}{output_filename}_{parameter}_{resource}")
    print("-----------------------")
    data.pop("result_avg")
    data["raw_result"] = data["result_sum"]
    data.pop("result_sum")
    pd.DataFrame.from_dict(data).to_csv(
        f'visualization/{OUTPUT_ADDRESS}{output_filename}_{parameter}_{resource}.csv'
    )
    plt.savefig(f'visualization/{OUTPUT_ADDRESS}{output_filename}_{parameter}_{resource}.jpg', bbox_inches='tight')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input")
    parser.add_argument("-t", "--type")
    parser.add_argument("-r", "--resource", default='a')
    parser.add_argument("-n", "--normalized", action='store_true', default=True)
    parser.add_argument("-avg", "--average", action='store_true', default=False)
    parser.add_argument("-cpu", "--cpu", action='store_true', default=False)
    parser.add_argument("-w", "--workload_number", type=int, required=False)
    parser.add_argument("-hr", "--hit_rate", type=float, required=False)
    args = parser.parse_args()

    # ===================================================
    # SETTING CONFIGS
    print(args.input)
    INPUT_ADDRESS = f'{args.input}'
    OUTPUT_ADDRESS = ''
    NORMALIZED = args.normalized
    WORKLOAD_NUMBER = args.workload_number
    HIT_RATE = args.hit_rate
    PARAMETER = PARAMETERS[args.type]
    RESOURCE = RESOURCES[args.resource]
    # ===================================================
    current_directory = os.getcwd()
    print(current_directory)
    directory_path = os.path.join(current_directory, INPUT_ADDRESS)
    files = [f for f in os.listdir(directory_path)]
    if DYNAMIC_VERSION:
        VERSIONS = []

    for f in files:
        version = f.title().split('.')[0][1:]
        if DYNAMIC_VERSION:
            VERSIONS.append(version)
        exec(f'jobs_{version} = pd.read_csv(f"{directory_path}/{f}").sort_values("job_id")')
        if PARAMETER == "user_fairness":
            exec(f"jobs_{version} = np.var(list(jobs_{version}.groupby('user_id').mean('slowdown_time')['slowdown_time']))")
        else:
            if RESOURCE:
                exec(f'jobs_{version} = jobs_{version}[jobs_{version}["partition"] == "{RESOURCE}"]')
            clean_data(eval(f'jobs_{version}'))
            if args.cpu:
                df = select_cpu_intensive_jobs(eval(f'jobs_{version}'))
                exec(f'jobs_{version} = df')

    VERSIONS = list(map(str, sorted(list(map(int, VERSIONS)))))

    labels = []
    data = {'version': [], 'result_sum': [], 'result_avg': []}
    for version in VERSIONS:
        exec(f'jobs_{version}_results_sum = []')
        exec(f'jobs_{version}_results_avg = []')
        # print(exec(f'jobs_{version}["{PARAMETER}"]'))
        if PARAMETER == "user_fairness":
            exec(f'jobs_{version}_param_sum = jobs_{version}')
            exec(f'jobs_{version}_param_mean = jobs_{version}')
        else:
            # exec(f'jobs_{version}["{PARAMETER}"] /= (60)')
            if PARAMETER == PARAMETERS['d']:
                exec(f'jobs_{version}_param_sum = jobs_{version}["{PARAMETER}"].max()')
            else:
                exec(f'jobs_{version}_param_sum = jobs_{version}["{PARAMETER}"].sum()')
            exec(f'jobs_{version}_param_mean = jobs_{version}["{PARAMETER}"].mean()')
        data['version'].append(version)
        data['result_sum'].append(eval(f'jobs_{version}_param_sum'))
        data['result_avg'].append(eval(f'jobs_{version}_param_mean'))
    if NORMALIZED:
        max_param_sum = max(eval(f'jobs_{version}_param_sum') for version in VERSIONS)
        max_param_mean = max(eval(f'jobs_{version}_param_mean') for version in VERSIONS)

        for version in VERSIONS:
            exec(f'jobs_{version}_param_sum /= max_param_sum')
            exec(f'jobs_{version}_param_mean /= max_param_mean')

    for version in VERSIONS:
        eval(f'jobs_{version}_results_sum').append(eval(f'jobs_{version}_param_sum'))
        eval(f'jobs_{version}_results_avg').append(eval(f'jobs_{version}_param_mean'))

    labels.append(f'{PARAMETER}')
    print(PARAMETER)

    draw(
        args.input,
        parameter=PARAMETER,
        resource=RESOURCE if RESOURCE else 'all',
        avg=args.average,
        data=data
    )
