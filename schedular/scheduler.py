import heapq
from typing import List, Set, Tuple

import pandas as pd

from job import Job
from partitions import Partition
from timer import TIMER


class Scheduler:
    def __init__(self, partition: Partition, number_of_nodes: int) -> None:
        self.partition = partition
        self.number_of_nodes = number_of_nodes
        self.number_of_free_nodes = number_of_nodes
        self.running_jobs: Set[Job] = set()
        self.waiting_jobs: List[Job] = []
        self.completed_jobs: Set[Job] = set()
        # this list is a heap where the first element has the earliest finish_time_estimate
        self.frontiers: List[Tuple[int, Job]] = []

    def on_job_submission(self, job: Job):
        self.waiting_jobs.append(job)

    def remove_completed_jobs(self):
        completed_jobs = []
        for j in self.running_jobs:
            if j.remaining_execution_time == 0:  # completed
                self.completed_jobs.add(j)
                self.number_of_free_nodes += 1
                completed_jobs.append(j)

        for j in completed_jobs:
            self.running_jobs.remove(j)

    def schedule(self):
        while self.number_of_free_nodes > 0 and len(self.waiting_jobs) > 0:
            j = self.waiting_jobs.pop(0)
            self.running_jobs.add(j)
            j.waiting_time = TIMER.global_time - j.submission_time
            j.is_added_to_running_list = True
            self.number_of_free_nodes -= 1

    def compute(self):
        for j in self.running_jobs:
            j.remaining_execution_time -= 1
        # todo: check for deletion
        # for j in self.waiting_jobs:
        #     j.waiting_time += 1

    def print_results(self) -> pd.DataFrame:
        return pd.DataFrame([j.to_dict() for j in self.completed_jobs])

    @property
    def is_free(self):
        if self.number_of_free_nodes > 0:
            return True
        return False

    def remove_completed_jobs_from_frontiers(self):
        frontiers_copy = self.frontiers.copy()
        self.frontiers.clear()
        for finish_time_estimate, job in frontiers_copy:
            if job not in self.completed_jobs:
                heapq.heappush(self.frontiers, (finish_time_estimate, job))

    def update_frontiers(self, job: Job):
        self.remove_completed_jobs_from_frontiers()
        if len(self.frontiers) < self.number_of_nodes:  # no job ahead! a free resource is available
            job.finish_time_estimate = job.submission_time + job.execution_times[self.partition]
            heapq.heappush(self.frontiers, (job.finish_time_estimate, job))
        else:  # has to wait on a job!
            earliest_finish_time, earliest_job = heapq.heappop(self.frontiers)
            job.finish_time_estimate = earliest_finish_time + job.execution_times[self.partition]
            heapq.heappush(self.frontiers, (job.finish_time_estimate, job))

    # def remove_a_running_job(self, job: Job):
    #     # job is not completed. should be removed for other reasons
    #     self.running_jobs.remove(job)
    #     self.number_of_free_nodes += 1
