# TODO Deprecated

import pandas as pd
import argparse
import math
import random


parser = argparse.ArgumentParser()
# mandantory arguments
parser.add_argument("-n", "--number_of_jobs", type=int, required=True)
parser.add_argument("-w", "--workload_number", type=int, required=True)
# optional arguments
parser.add_argument("-uv", "--user_partitioning_version", type=str, required=False, choices=['rich', 'eco', 'nocache', 'onlycache', 'mix'], default=None)
parser.add_argument("--hdd_rate", type=float, required=False, default=None)
parser.add_argument("--cache_rate", type=float, required=False, default=None)
parser.add_argument("-cr", "--cpu_rate", type=float, required=False, default=0)
parser.add_argument("-hr", "--hit_rate", type=str, required=False, choices=['high', 'mid', 'low'], default=None)
parser.add_argument("-fr", "--famous_rate", type=float, required=False, choices=[0.1, 0.2, 0.3], default=None)
args = parser.parse_args()

NUMBER_OF_JOBS = args.number_of_jobs
WORKLOAD_NUMBER = args.workload_number
USER_PARTITIONING_VERSION = args.user_partitioning_version
if USER_PARTITIONING_VERSION == 'mix':
    assert args.hdd_rate, f'you have not specified % of users that select HDD'
    assert args.cache_rate, f'you have not specified % of users that select CACHE'
    HDD_USERS_RATE = args.hdd_rate
    CACHE_USERS_RATE = args.cache_rate
    assert (HDD_USERS_RATE + CACHE_USERS_RATE) == 1, f'sum of percents for different partitions is not equal to 1'

CPU_RATE = args.cpu_rate
IO_RATE = 1 - CPU_RATE
HIT_RATE = args.hit_rate
FAMOUS_RATE = args.famous_rate
INPUT_FILE_NAME = f'../inputs/10jobs_low_hit.csv'  # the main file
if HIT_RATE not in INPUT_FILE_NAME:
    raise f"************\nWARNING: your input file name ({INPUT_FILE_NAME}) maybe wrong!************\n"

OUTPUT_FILENAME = f'{NUMBER_OF_JOBS}jobs'

if USER_PARTITIONING_VERSION:
    OUTPUT_FILENAME += f'_user_partitioning_{USER_PARTITIONING_VERSION}'
    if USER_PARTITIONING_VERSION == 'mix':
        OUTPUT_FILENAME += f'_{HDD_USERS_RATE}HDD_{CACHE_USERS_RATE}CACHE'
if HIT_RATE:
    OUTPUT_FILENAME += f'_{HIT_RATE}_hit'
if FAMOUS_RATE:
    OUTPUT_FILENAME += f'_{FAMOUS_RATE}_famous'
if CPU_RATE:
    OUTPUT_FILENAME += f'_cpu_rate{CPU_RATE}'

OUTPUT_FILENAME += f'_{WORKLOAD_NUMBER}.csv'

CPU_INTENSIVE_APPS = {
    'graphsage',
    'dcn',
    'gat',
    'deepfm',
}

def get_random_partition():
    PARTITIONS = [
        'cache_with_hdd',
        'central_hdd',
        'local_ssd'
    ]
    return random.choice(PARTITIONS)

def dataset_is_famous(dataset_name: str):
    FAMOUS_DATASETS = {
        'ImageNet',
        'MIT_Places'
    }
    return dataset_name in FAMOUS_DATASETS

def set_user_partitioning(df: pd.DataFrame):
    if USER_PARTITIONING_VERSION is None:
        pass
    elif USER_PARTITIONING_VERSION == 'rich':
        df['partition'] = df.apply(lambda x: 'local_ssd' if dataset_is_famous(x['dataset_name']) else 'cache_with_hdd', axis=1)
    elif USER_PARTITIONING_VERSION == 'eco':
        df['partition'] = 'central_hdd'
    elif USER_PARTITIONING_VERSION == 'nocache':
        df['partition'] = df.apply(lambda x: 'local_ssd' if dataset_is_famous(x['dataset_name']) else 'central_hdd', axis=1)
    elif USER_PARTITIONING_VERSION == 'onlycache':
        df['partition'] = 'cache_with_hdd'
    elif USER_PARTITIONING_VERSION == 'mix':
        df['partition'] = df.apply(lambda x: 'central_hdd' if random.random() < HDD_USERS_RATE else 'cache_with_hdd', axis=1)

df = pd.read_csv(INPUT_FILE_NAME)

if CPU_RATE:
    # jobs with CPU-intensive applications
    df_cpu = df[df.apply(lambda x: x.application_name in CPU_INTENSIVE_APPS, axis=1)]
    # jobs with IO-intensive applications
    df_io = df[df.apply(lambda x: x.application_name not in CPU_INTENSIVE_APPS, axis=1)]

    # scale the job and sample based on CPU-rate
    if len(df_cpu) > 0:
        df_cpu_scaled = pd.concat([df_cpu] * 2 * int(NUMBER_OF_JOBS // len(df_cpu))).sample(n=math.ceil(NUMBER_OF_JOBS * CPU_RATE))
    else:
        df_cpu_scaled = df_cpu
    if len(df_io) > 0:
        df_io_scaled = pd.concat([df_io] * 2 * int(NUMBER_OF_JOBS  // len(df_io))).sample(n=math.floor(NUMBER_OF_JOBS * IO_RATE))
    else:
        df_io_scaled = df_io

    df = pd.concat([df_cpu_scaled, df_io_scaled]).reset_index()
else:
    assert NUMBER_OF_JOBS % len(df) == 0, 'number of jobs must be a multiple of number of jobs in the intial dataset'
    df = pd.concat([df] * (NUMBER_OF_JOBS // len(df)))

COLUMNS = ['id', 'submission_time', 'partition',
       'application_name', 'dataset_name', 'read_io','execution_time_ssd', 'execution_time_hdd',
       'execution_time_cache']

df = df[COLUMNS]
# randomizing the order of jobs
df = df.sample(frac=1).reset_index()
# setting ID and submission time
df = df.drop(['index'], axis = 1)
df['id'] = df.index + 1
# df['submission_time'] = df.index + 1
df['submission_time'] = 1

set_user_partitioning(df)

df.to_csv(OUTPUT_FILENAME)
