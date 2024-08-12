# TODO Deprecated

import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-n", "--number_of_jobs", type=int, required=True)
parser.add_argument("-w", "--workload_number", type=int, required=True)
args = parser.parse_args()
number_of_jobs = args.number_of_jobs
workload_number = args.workload_number
output_file_name = f'{number_of_jobs}jobs{workload_number}.csv'
df = pd.read_csv(f'15jobs1.csv') # the main file
INITIAL_JOB_SIZE = len(df)
assert number_of_jobs % INITIAL_JOB_SIZE == 0, 'number of jobs must be a multiple of 15'
df = pd.concat([df] * (number_of_jobs // INITIAL_JOB_SIZE)).reset_index()
COLUMNS = ['id', 'submission_time', 'execution_time_ssd', 'partition',
       'application_name', 'dataset_name', 'read_io', 'execution_time_hdd',
       'execution_time_cache']
df = df[COLUMNS]
df = df.sample(frac=1).reset_index()
df = df.drop(['index'], axis = 1)
df['id'] = df.index + 1
df['submission_time'] = df.index + 1
df.to_csv(output_file_name)