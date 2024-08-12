# TODO Deprecated

import pandas as pd
import argparse
import math
import random

parser = argparse.ArgumentParser()
# mandantory arguments
parser.add_argument("-n", "--number_of_jobs", type=int, required=True)
parser.add_argument("-w", "--workload_number", type=int, required=True)
parser.add_argument("-hr", "--hit_rate", type=float, required=True, default=None)
parser.add_argument("-fr", "--famous_rate", type=float, required=True, default=None, choices=[0.1, 0.2, 0.3])
parser.add_argument("-uv", "--user_partitioning_version", type=str, required=True, choices=['rich', 'eco', 'nocache', 'onlycache', 'mix'], default=None)
# other arguments
parser.add_argument("--hdd_rate", type=float, required=False, default=None)
parser.add_argument("--cache_rate", type=float, required=False, default=None)

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
    USER_PARTITIONING_VERSION += f'_{HDD_USERS_RATE}HDD_{CACHE_USERS_RATE}CACHE'

HIT_RATE = args.hit_rate # this arg indicates the % of high hit jobs in the generated output
FAMOUS_RATE = args.famous_rate # this arg indicates the % of jobs with famous data set in the generated output
# input files
INPUT_FILE_NAME_HIGH_HIT = f'{NUMBER_OF_JOBS}jobs_user_partitioning_{USER_PARTITIONING_VERSION}_high_hit_{FAMOUS_RATE}_famous_{WORKLOAD_NUMBER}.csv'
INPUT_FILE_NAME_LOW_HIT = f'{NUMBER_OF_JOBS}jobs_user_partitioning_{USER_PARTITIONING_VERSION}_low_hit_{FAMOUS_RATE}_famous_{WORKLOAD_NUMBER}.csv'

OUTPUT_FILENAME = f'{NUMBER_OF_JOBS}jobs_user_partitioning_{USER_PARTITIONING_VERSION}_{HIT_RATE}_hit_{FAMOUS_RATE}_famous_{WORKLOAD_NUMBER}.csv'


df_high_hit = pd.read_csv(INPUT_FILE_NAME_HIGH_HIT)
df_low_hit = pd.read_csv(INPUT_FILE_NAME_LOW_HIT)

assert len(df_high_hit) > 0 and len(df_low_hit) > 0, 'Your input files are empty!'

# scale the job and sample based on CPU-rate
df_high_hit = df_high_hit.sample(n=math.ceil(NUMBER_OF_JOBS * HIT_RATE))
df_low_hit = df_low_hit.sample(n=math.floor(NUMBER_OF_JOBS * (1 - HIT_RATE)))

# df = pd.concat([df_high_hit, df_low_hit]).reset_index()
df = pd.concat([df_low_hit, df_high_hit]).reset_index()
COLUMNS = ['id', 'submission_time', 'partition',
       'application_name', 'dataset_name', 'read_io','execution_time_ssd', 'execution_time_hdd',
       'execution_time_cache']

df = df[COLUMNS]
# randoming order of jobs
df = df.sample(frac=1).reset_index()
# setting ID and submission time
# df = df.reset_index()
df = df.drop(['index'], axis = 1)
df['id'] = df.index + 1
# df['submission_time'] = df.index + 1
df['submission_time'] = 1

df.to_csv(OUTPUT_FILENAME)
