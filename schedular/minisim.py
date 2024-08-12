import itertools
import os
from time import time
from typing import Dict, List, Set

import pandas as pd
from tqdm import tqdm

from application_manager import APPLICATIONS
from dataset_manager import is_dataset_famous
from job import Job
from job_parser import parse_csv
from minisim_config import MINISIM_CONFIG
from partitions import Partition
from scheduler import Scheduler
from timer import TIMER


class Simulator:
    def __init__(self) -> None:
        if MINISIM_CONFIG.input_file:
            self.jobs = parse_csv(MINISIM_CONFIG.input_file)
        elif MINISIM_CONFIG.workload_number:
            file_name = f'inputs/{MINISIM_CONFIG.number_of_jobs}jobs'
            if MINISIM_CONFIG.user_partitioning_version:
                file_name += f'_user_partitioning_{MINISIM_CONFIG.user_partitioning_version}'
            if MINISIM_CONFIG.hit_rate:
                file_name += f'_{MINISIM_CONFIG.hit_rate}_hit'
            if MINISIM_CONFIG.famous_rate:
                file_name += f'_{MINISIM_CONFIG.famous_rate}_famous'
            if MINISIM_CONFIG.cpu_rate:
                file_name += f'_cpu_rate{MINISIM_CONFIG.cpu_rate}'
            file_name += f'_{MINISIM_CONFIG.workload_number}.csv'
            print(file_name)
            MINISIM_CONFIG.input_file = file_name
            self.jobs = parse_csv(file_name)
        else:
            raise "please clarify the input file, or get the donfig to generate input file"

        nodes = {p: MINISIM_CONFIG.number_of_nodes[p] for p in list(Partition)}
        if MINISIM_CONFIG.SCHEDULING_VERSION in (1, 21):
            nodes[Partition.HDD] += nodes[Partition.SSD]
            nodes[Partition.SSD] = 0
        self.schedulers: Dict[Partition, Scheduler] = {p: Scheduler(p, nodes[p]) for p in list(Partition)}
        self.waiting_set_between_partitions: Set[Job] = set()

    def check_new_job_arrival(self) -> List[Job]:
        results = []
        while len(self.jobs) > 0 and self.jobs[0].submission_time == TIMER.global_time:
            results.append(self.jobs.pop(0))

        return results

    def schedule(self):
        if MINISIM_CONFIG.SCHEDULING_VERSION == 0:
            # in this version only HDD nodes exist
            self.schedulers[Partition.HDD].schedule()
        else:
            for p in list(Partition):
                self.schedulers[p].schedule()

    def compute(self):
        for p in list(Partition):
            self.schedulers[p].compute()

    def remove_completed_jobs(self):
        for p in list(Partition):
            self.schedulers[p].remove_completed_jobs()

    def simulation_is_end(self) -> bool:
        if len(self.jobs) > 0:
            return False
        else:  # all jobs have been submitted
            for p in list(Partition):  # check for running_jobs
                if len(self.schedulers[p].running_jobs) > 0:
                    return False
                if len(self.schedulers[p].waiting_jobs) > 0:
                    return False
            return True

    # TODO Deprecated
    def waiting_time_for_cache(self) -> int:
        frontiers = self.schedulers[Partition.CACHE].frontiers
        if len(frontiers) < self.schedulers[Partition.CACHE].number_of_nodes:  # ther is a free node
            return 0
        else:  # no free node
            earliest_finish_time, job = frontiers[
                0]  # frontier is a heap so the first element represents the earliest finish_time_estimate
            return max(earliest_finish_time - TIMER.global_time, 0)

    # TODO Deprecated
    def waiting_time_for_local_ssd(self) -> int:
        frontiers = self.schedulers[Partition.SSD].frontiers
        if len(frontiers) < self.schedulers[Partition.SSD].number_of_nodes:  # there is a free node
            return 0
        else:  # no free node
            earliest_finish_time, job = frontiers[
                0]  # frontier is a heap so the first element represents the earliest finish_time_estimate
            return max(earliest_finish_time - TIMER.global_time, 0)

    # TODO Deprecated
    def waiting_time_for_hdd(self) -> int:
        frontiers = self.schedulers[Partition.HDD].frontiers
        if len(frontiers) < self.schedulers[Partition.HDD].number_of_nodes:  # there is a free node
            return 0
        else:  # no free node
            earliest_finish_time, job = frontiers[
                0]  # frontier is a heap so the first element represents the earliest finish_time_estimate
            return max(earliest_finish_time - TIMER.global_time, 0)

    def waiting_time(self, p: Partition) -> int:
        frontiers = self.schedulers[p].frontiers
        if len(frontiers) < self.schedulers[p].number_of_nodes:
            return 0
        else:  # no free node
            earliest_finish_time, job = frontiers[
                0]  # frontier is a heap so the first element represents the earliest finish_time_estimate
            return max(earliest_finish_time - TIMER.global_time, 0)

    # TODO Deprecated
    def is_cache_faster_than_hdd(self, job: Job) -> bool:
        total_time_on_hdd = APPLICATIONS[job.application_name].total_time_on_hdd(job.total_io)
        total_time_on_cache = APPLICATIONS[job.application_name].time_on_cache(job.total_io)
        waiting_time_on_cache = self.waiting_time_for_cache()
        waiting_time_on_hdd = self.waiting_time_for_hdd()

        if total_time_on_cache + waiting_time_on_cache < total_time_on_hdd + waiting_time_on_hdd:
            return True
        return False

    # TODO Deprecated
    def is_local_ssd_faster_than_hdd(self, job: Job) -> bool:
        total_time_on_local_ssd = APPLICATIONS[job.application_name].total_time_on_ssd(job.total_io)
        total_time_on_hdd = APPLICATIONS[job.application_name].total_time_on_hdd(job.total_io)
        waiting_time_on_local_ssd = self.waiting_time_for_local_ssd()
        waiting_time_on_hdd = self.waiting_time_for_hdd()
        if total_time_on_local_ssd + waiting_time_on_local_ssd < total_time_on_hdd + waiting_time_on_hdd:
            return True
        else:
            return False

    # TODO Deprecated
    def estimate_fastest_partition(self, job: Job) -> Partition:
        estimated_execution_time_on_local_ssd = APPLICATIONS[job.application_name].total_time_on_ssd(job.total_io)
        estimated_execution_time_on_cache = APPLICATIONS[job.application_name].time_on_cache(job.total_io)
        estimated_time_on_hdd = APPLICATIONS[job.application_name].total_time_on_hdd(job.total_io)
        waiting_time_on_local_ssd = self.waiting_time_for_local_ssd()
        waiting_time_on_cache = self.waiting_time_for_cache()
        waiting_time_on_hdd = self.waiting_time_for_hdd()

        turnaround_cache = estimated_execution_time_on_cache + waiting_time_on_cache
        turnaround_local_ssd = estimated_execution_time_on_local_ssd + waiting_time_on_local_ssd
        turnaround_hdd = estimated_time_on_hdd + waiting_time_on_hdd
        min_turnaround_time = min(turnaround_cache, turnaround_hdd, turnaround_local_ssd)
        if turnaround_cache == min_turnaround_time:
            return Partition.CACHE
        if turnaround_hdd == min_turnaround_time:
            return Partition.HDD
        if turnaround_local_ssd == min_turnaround_time:
            return Partition.SSD

    def select_fastest_partition(self, job: Job) -> Partition:
        famous = is_dataset_famous(job.dataset_name)
        resource = {}
        for p in list(Partition):
            if self.schedulers[p].number_of_nodes > 0:
                resource[p] = job.execution_times[p] + self.waiting_time(p)
        return min(resource, key=lambda k: resource[k]) if len(resource) else Partition.SSD

    def determine_partition(self, job: Job) -> Partition:
        if MINISIM_CONFIG.SCHEDULING_VERSION == 0:  # SAN HDD
            return Partition.HDD

        if MINISIM_CONFIG.SCHEDULING_VERSION == 1:  # user partitioning
            return job.partition

        if MINISIM_CONFIG.SCHEDULING_VERSION in (11, 12):  # FC-FS
            wait_times = {
                Partition.HDD: self.waiting_time_for_hdd(),
                Partition.CACHE: self.waiting_time_for_cache(),
                Partition.SSD: self.waiting_time_for_local_ssd(),
            }
            return min(wait_times, key=wait_times.get)

        elif MINISIM_CONFIG.SCHEDULING_VERSION == 21:
            if job.application.is_high_hit:
                return Partition.CACHE
            else:
                return Partition.HDD

        elif MINISIM_CONFIG.SCHEDULING_VERSION == 25:
            if not is_dataset_famous(job.dataset_name):
                if job.application.is_high_hit:
                    return Partition.CACHE
                else:
                    return Partition.HDD
            else:  # famous dataset
                return Partition.SSD

        elif MINISIM_CONFIG.SCHEDULING_VERSION == 29:
            if is_dataset_famous(job.dataset_name):
                if self.schedulers[Partition.SSD].is_free:
                    return Partition.SSD
                else:
                    if job.application.is_high_hit:
                        if self.schedulers[Partition.CACHE].is_free:
                            return Partition.CACHE
                        else:
                            return self.select_fastest_partition(job)
                    else:
                        execution_time_on_ssd = job.execution_times[Partition.SSD]
                        execution_time_on_hdd = job.execution_times[Partition.HDD]
                        waiting_time_on_ssd = self.waiting_time_for_local_ssd()
                        waiting_time_on_hdd = self.waiting_time_for_hdd()

                        turnaround_ssd = execution_time_on_ssd + waiting_time_on_ssd
                        turnaround_hdd = execution_time_on_hdd + waiting_time_on_hdd
                        if turnaround_hdd <= turnaround_ssd:
                            return Partition.HDD
                        else:
                            return Partition.SSD
            else:
                if job.application.is_high_hit:
                    if self.schedulers[Partition.CACHE].is_free:
                        return Partition.CACHE
                    else:
                        execution_time_on_cache = job.execution_times[Partition.CACHE]
                        execution_time_on_hdd = job.execution_times[Partition.HDD]
                        waiting_time_on_cache = self.waiting_time_for_cache()
                        waiting_time_on_hdd = self.waiting_time_for_hdd()

                        turnaround_cache = execution_time_on_cache + waiting_time_on_cache
                        turnaround_hdd = execution_time_on_hdd + waiting_time_on_hdd
                        if turnaround_cache < turnaround_hdd:
                            return Partition.CACHE
                        else:
                            return Partition.HDD
                else:
                    return Partition.HDD

        elif MINISIM_CONFIG.SCHEDULING_VERSION == 4:  # ideal version of smart-partitioning
            return self.select_fastest_partition(job)

    # TODO Deprecated
    def determine_multiple_partitions(self, jobs):
        ft = time()
        all_possible_partition_permutations = self.__get_all_possible_permutations__(batch_size=len(jobs))
        job_permutations = list(itertools.permutations(list(range(len(jobs)))))
        best_permutation = None
        best_job_shuffled = None
        best_turnaround_time = float('inf')
        for permutation in tqdm(all_possible_partition_permutations):
            for job_indexing in job_permutations:
                job_shuffled = [jobs[i] for i in job_indexing]
                if not self.__validate_permutation__(permutation, job_shuffled):
                    continue
                total_turnaround_time, _ = self.__calculate_turnaround_permutations__(permutation, job_shuffled)
                if total_turnaround_time < best_turnaround_time:
                    best_turnaround_time = total_turnaround_time
                    best_permutation = permutation
                    best_job_shuffled = job_shuffled
        print(f"one batch time: {time() - ft}")
        return best_permutation, best_job_shuffled

    # TODO Deprecated
    def determine_multiple_partitions_heuristic(self, jobs):
        ft = time()
        all_possible_partition_permutations = self.__get_all_possible_permutations__(batch_size=len(jobs))
        possible_permutation = {}

        for p in tqdm(all_possible_partition_permutations):
            if not self.__validate_permutation__(p, jobs):
                continue
            total_turnaround_time, huge_job_index = self.__calculate_turnaround_permutations__(p, jobs)

            if len(possible_permutation) == 0 or total_turnaround_time == list(possible_permutation.values())[0][0]:
                possible_permutation[p] = (total_turnaround_time, huge_job_index)
            elif total_turnaround_time < list(possible_permutation.values())[0][0]:
                possible_permutation.clear()
                possible_permutation[p] = (total_turnaround_time, huge_job_index)

        values = {
            Partition.SSD: self.schedulers[Partition.SSD].number_of_nodes,
            Partition.CACHE: self.schedulers[Partition.CACHE].number_of_nodes,
            Partition.HDD: self.schedulers[Partition.HDD].number_of_nodes,
        }

        best_permutation = tuple()
        best_value = float('inf')
        huge_job_index = None

        for p, v in possible_permutation.items():
            value_count = 0
            for partition in p:
                value_count += values[partition]
            if value_count < best_value:
                best_value = value_count
                best_permutation = list(p)
                huge_job_index = v[1]

        huge_job = jobs.pop(huge_job_index)
        huge_job_partition = best_permutation.pop(huge_job_index)
        jobs.append(huge_job)
        best_permutation.append(huge_job_partition)

        print(f"one batch time: {time() - ft}")
        return best_permutation, jobs

    # TODO Deprecated
    def __get_all_possible_permutations__(self, batch_size: int) -> List[Partition]:
        available_partitions = []
        for p in list(Partition):
            if self.schedulers[p].number_of_nodes > 0:
                available_partitions.append(p)
        return [p for p in itertools.product(available_partitions, repeat=batch_size)]

    # TODO Deprecated
    def __calculate_turnaround_permutations__(self, permutation: List[Partition], jobs: List[Job]):
        # making a copy of current state of frontiers
        original_frontier_ssd = self.schedulers[Partition.SSD].frontiers.copy()
        original_frontier_hdd = self.schedulers[Partition.HDD].frontiers.copy()
        original_frontier_cache = self.schedulers[Partition.CACHE].frontiers.copy()

        huge_job_index = 0
        huge_job_time = 0
        total_turnaround_time = 0
        index = 0
        for p, j in zip(permutation, jobs):
            exec_time = j.execution_times[p]
            waiting_time = self.waiting_time(p)
            total_turnaround_time += exec_time + waiting_time
            if exec_time > huge_job_time:
                huge_job_index = index
                huge_job_time = total_turnaround_time
            index += 1
            self.schedulers[p].update_frontiers(j)

        # reseting frontiers to their original state
        self.schedulers[Partition.SSD].frontiers = original_frontier_ssd
        self.schedulers[Partition.HDD].frontiers = original_frontier_hdd
        self.schedulers[Partition.CACHE].frontiers = original_frontier_cache

        return total_turnaround_time, huge_job_index

    # TODO Deprecated
    @staticmethod
    def __validate_permutation__(permutation, job_shuffled):
        for p, j in zip(permutation, job_shuffled):
            if not is_dataset_famous(j.dataset_name) and p == Partition.SSD:
                return False
        return True

    def simulate(self):
        while not self.simulation_is_end():
            TIMER.global_time += 1
            new_jobs = self.check_new_job_arrival()
            if not MINISIM_CONFIG.MULTI_JOB_SCHEDULING:
                for new_job in new_jobs:
                    partition = self.determine_partition(new_job)
                    new_job.partition = partition
                    new_job.remaining_execution_time = new_job.execution_times[new_job.partition]
                    self.schedulers[new_job.partition].on_job_submission(new_job)
                    if MINISIM_CONFIG.SCHEDULING_VERSION == 11 or \
                            MINISIM_CONFIG.SCHEDULING_VERSION == 29 or \
                            MINISIM_CONFIG.SCHEDULING_VERSION == 4:
                        self.schedulers[new_job.partition].update_frontiers(new_job)
                    self.schedule()

            else:  # multi job scheduling
                if MINISIM_CONFIG.SCHEDULING_VERSION == 12:
                    jobs_avg_time = {}
                    for job in new_jobs:
                        jobs_avg_time[job.id] = (
                                                        job.execution_times[Partition.HDD] +
                                                        job.execution_times[Partition.CACHE] +
                                                        job.execution_times[Partition.SSD]
                                                ) // 3
                    jobs_avg_time = sorted(jobs_avg_time.items(), key=lambda x: x[1])
                    jobs_to_schedule = []
                    partitions = []
                    for j2 in jobs_avg_time:
                        for j in new_jobs:
                            if j.id == j2[0]:
                                p = self.determine_partition(j)
                                j.partition = p
                                j.remaining_execution_time = j.execution_times[j.partition]
                                self.schedulers[j.partition].on_job_submission(j)
                                self.schedulers[j.partition].update_frontiers(j)
                                self.schedule()
                if MINISIM_CONFIG.SCHEDULING_VERSION == 41:
                    BATCH_SIZE = MINISIM_CONFIG.batch_size
                    for i in range(0, len(new_jobs), BATCH_SIZE):  # select a batch of jobs
                        jobs_to_schedule = new_jobs[i: i + BATCH_SIZE]
                        partitions, jobs_to_schedule = self.determine_multiple_partitions(jobs_to_schedule)
                        for j, p in zip(jobs_to_schedule, partitions):
                            j.partition = p
                            j.remaining_execution_time = j.execution_times[j.partition]
                            self.schedulers[j.partition].on_job_submission(j)
                            self.schedulers[j.partition].update_frontiers(j)
                            self.schedule()
                elif MINISIM_CONFIG.SCHEDULING_VERSION == 42:
                    BATCH_SIZE = MINISIM_CONFIG.batch_size
                    for i in range(0, len(new_jobs), BATCH_SIZE):  # select a batch of jobs
                        jobs_to_schedule = new_jobs[i: i + BATCH_SIZE]
                        partitions, jobs_to_schedule = self.determine_multiple_partitions_heuristic(jobs_to_schedule)
                        for j, p in zip(jobs_to_schedule, partitions):
                            j.partition = p
                            j.remaining_execution_time = j.execution_times[j.partition]
                            self.schedulers[j.partition].on_job_submission(j)
                            self.schedulers[j.partition].update_frontiers(j)
                            self.schedule()

            self.schedule()
            self.compute()
            self.remove_completed_jobs()

        self.print_results()

    def print_results(self):
        output_df = pd.DataFrame()
        for p in list(Partition):
            df_partition = self.schedulers[p].print_results()
            output_df = pd.concat([output_df, df_partition], axis=0)
        output_df['starting_time'] = output_df['submission_time'] + output_df['waiting_time']
        output_df['turnaround_time'] = output_df['waiting_time'] + output_df['execution_time']
        output_df['finish_time'] = output_df['starting_time'] + output_df['execution_time']
        output_df['slowdown_time'] = output_df['turnaround_time'] / output_df['execution_time']
        output_df = output_df.sort_values(by=['submission_time', 'job_id'])
        directory = 'output/' + MINISIM_CONFIG.input_file.split('/')[1][:-4]
        if not os.path.exists(directory):
            os.makedirs(directory)
        output_file = f'{directory}/v{MINISIM_CONFIG.SCHEDULING_VERSION}.csv'
        print(output_file)
        output_df.to_csv(output_file)


if __name__ == '__main__':
    first_time = time()
    simulator = Simulator()
    simulator.simulate()
    print(f"overall time: {time() - first_time}s")
