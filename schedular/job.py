from typing import Dict, List

from application_manager import APPLICATIONS, Application
from dataset_manager import is_dataset_famous
from partitions import Partition


class Job:
    def __init__(self,
                 id,
                 user_id: int,
                 submission_time: int,
                 execution_time_ssd: int,
                 execution_time_hdd: int,
                 execution_time_cache: int,
                 partition: str,
                 application_name: str = None,
                 dataset_name: str = None,
                 read_io: int = None) -> None:
        self.id = id
        self.user_id = user_id
        self.submission_time = submission_time
        self.execution_times = {
            Partition.SSD: execution_time_ssd if is_dataset_famous(dataset_name) else execution_time_hdd,
            Partition.HDD: execution_time_hdd,
            Partition.CACHE: execution_time_cache,
        }
        assert len(self.execution_times) == len(
            list(Partition)), 'execution time for each partition must be specified. some partitions are missing!'
        self.partition = Partition(partition)
        self.old_partition = Partition(partition)
        self.remaining_execution_time = None
        self.waiting_time = 0
        self.application_name = application_name
        self.dataset_name = dataset_name
        self.read_io = read_io
        self.waiting_partitions: List[Partition] = None
        self.is_added_to_running_list: bool = False
        self.finish_time_estimate = None

    def __repr__(self) -> str:
        return f'id: {self.id}, sub: {self.submission_time}\n'

    def to_dict(self) -> Dict:
        return {
            'job_id': self.id,
            'user_id': self.user_id,
            'user_partition': self.old_partition.value,
            'partition': self.partition.value,
            'submission_time': self.submission_time,
            'execution_time': self.execution_times[self.partition],
            'waiting_time': self.waiting_time,
            'application_name': self.application_name,
            'dataset_name': self.dataset_name,
            'read_io': self.read_io,
            'io_intensive': self.is_io_intensive,
        }

    @property
    def is_io_intensive(self) -> bool:
        if self.read_io is not None and self.read_io < 10e9:
            return False  # CPU-intnesive

        if self.dataset_name is None and self.read_io is None:
            return False  # CPU-intensive

        return True  # IO-intensive

    @property
    def is_waiting_for_partitions(self) -> bool:
        if self.waiting_partitions is None:
            return False
        return True

    @property
    def is_sequential(self) -> bool:
        if self.application_name in APPLICATIONS:
            return APPLICATIONS[self.application_name].is_sequential
        raise "could not find your application!"

    @property
    def application(self) -> Application:
        # assert self.application_name in APPLICATIONS, f"could not find your application!, name: {self.application_name}"
        if self.application_name in APPLICATIONS:
            return APPLICATIONS[self.application_name]
        return APPLICATIONS['other']

    @property
    def total_io(self):
        # this should change if write_io is involved.
        return self.read_io

    def __lt__(self, other: 'Job'):
        return self.id < other.id
