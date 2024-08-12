from typing import List

import pandas as pd

from job import Job


def parse_csv(input_filename: str) -> List[Job]:
    jobs = []
    df = pd.read_csv(input_filename)
    df = df.sort_values(by=['submission_time', 'id'])
    df = df.apply(lambda j: create_job(j, jobs), axis=1)
    return jobs


def create_job(entry: pd.Series, jobs: List):
    # we assume that all of the following fields are provided
    job_id = entry['id']
    user_id = entry['user_id']
    submission_time = entry['submission_time']
    execution_time_ssd = entry['execution_time_ssd']
    execution_time_hdd = entry['execution_time_hdd']
    execution_time_cache = entry['execution_time_cache']
    partition = entry['partition']
    dataset_name = None if pd.isna(entry['dataset_name']) else entry['dataset_name']
    application_name = entry['application_name']
    read_io = None if pd.isna(entry['read_io']) else float(entry['read_io'])
    j = Job(job_id,
            user_id,
            submission_time,
            execution_time_ssd,
            execution_time_hdd,
            execution_time_cache,
            partition,
            application_name,
            dataset_name,
            read_io)
    jobs.append(j)
