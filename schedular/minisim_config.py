import argparse
import configparser
from typing import Dict

from partitions import Partition


class MinisimConfig:
    MULTI_JOB_SCHEDULER_VERSIONS = {
        41, 42, 12
    }

    def __init__(self) -> None:
        config = configparser.RawConfigParser()
        config.read('config.txt')
        self.set_configs(config)
        self.print_configs()

    def set_configs(self, config: configparser.RawConfigParser):
        self.SCHEDULING_VERSION = config.getint('DEFAULT', 'scheduling_version')
        self.SSD_BW = int(config.getfloat('DEFAULT', 'SSD_BW'))
        self.HDD_BW = int(config.getfloat('DEFAULT', 'HDD_BW'))
        self.SSD_BLOCK_SIZE = int(config.getfloat('DEFAULT', 'SSD_BLOCK_SIZE'))
        self.HDD_BLOCK_SIZE = int(config.getfloat('DEFAULT', 'HDD_BLOCK_SIZE'))
        self.SSD_IOPS = int(config.getfloat('DEFAULT', 'SSD_IOPS'))
        self.HDD_IOPS = int(config.getfloat('DEFAULT', 'HDD_IOPS'))

        parser = argparse.ArgumentParser()
        parser.add_argument("-w", "--workload_number", type=int, required=False, default=None)
        parser.add_argument("-n", "--number_of_jobs", type=int, required=False, default=None)
        parser.add_argument("-v", "--version", type=int, required=False, default=None)
        parser.add_argument("-uv", "--user_partitioning_version", type=str, required=False, default=None,
                            choices=['rich', 'eco', 'nocache', 'onlycache', 'mix'])
        parser.add_argument("--hdd_rate", type=float, required=False, default=None)
        parser.add_argument("--cache_rate", type=float, required=False, default=None)

        parser.add_argument("-cr", "--cpu_rate", type=float, required=False, default=None)
        parser.add_argument("-hr", "--hit_rate", type=str, required=False, default=None,
                            choices=['high', 'mid', 'low', '0.5', '0.7', '0.3', '0.2', '0.8'])
        parser.add_argument("-fr", "--famous_rate", type=float, required=False, default=None, choices=[0.1, 0.2, 0.3])
        parser.add_argument("-bs", "--batch_size", type=int, required=False, default=None)
        parser.add_argument("-f", "--file", type=str, required=False, default=None)
        args = parser.parse_args()
        self.workload_number = args.workload_number
        self.number_of_jobs = args.number_of_jobs
        self.cpu_rate = args.cpu_rate
        self.hit_rate = args.hit_rate
        self.famous_rate = args.famous_rate
        self.user_partitioning_version = args.user_partitioning_version
        self.input_file = args.file
        if self.user_partitioning_version == 'mix':
            assert args.hdd_rate, f'you have not specified % of users that select HDD'
            assert args.cache_rate, f'you have not specified % of users that select CACHE'
            self.hdd_users_rate = args.hdd_rate
            self.cache_users_rate = args.cache_rate
            assert (
                               self.cache_users_rate + self.hdd_users_rate) == 1, f'sum of percents for different partitions is not equal to 1'
            self.user_partitioning_version += f'_{self.hdd_users_rate}HDD_{self.cache_users_rate}CACHE'

        self.batch_size = args.batch_size
        if args.version is not None:
            self.SCHEDULING_VERSION = args.version
        self.MULTI_JOB_SCHEDULING: bool = self.SCHEDULING_VERSION in self.MULTI_JOB_SCHEDULER_VERSIONS

        self.number_of_nodes: Dict[Partition, int] = {}
        all_node_count = 0
        for p in list(Partition):
            node_count = int(config.getint('DEFAULT', p.name + '_node_count'))
            all_node_count += node_count
            self.number_of_nodes[p] = node_count

        if self.SCHEDULING_VERSION == 0:  # all nodes would be HDD and all jobs will be scheduled on them
            self.number_of_nodes = {
                Partition.SSD: 0,
                Partition.CACHE: 0,
                Partition.HDD: all_node_count
            }

    def print_configs(self):
        print('######################################')
        print(f'Scheduling Version: {self.SCHEDULING_VERSION}')
        print(f'User Partitioning Version: {self.user_partitioning_version}')
        print(f'Workload Number: {self.workload_number}')
        print(f'SSD_BW: {self.SSD_BW}')
        print(f'HDD_BW: {self.HDD_BW}')
        print(f'SSD_BLOCK_SIZE: {self.SSD_BLOCK_SIZE}')
        print(f'HDD_BLOCK_SIZE: {self.HDD_BLOCK_SIZE}')
        print(f'SSD_IOPS: {self.SSD_IOPS}')
        print(f'HDD_IOPS: {self.HDD_IOPS}')
        for p, nb in self.number_of_nodes.items():
            print(f'number of nodes {p}: {nb}')


MINISIM_CONFIG = MinisimConfig()
