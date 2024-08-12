from enum import Enum


class Partition(Enum):
    HDD = 'central_hdd'
    CACHE = 'cache_with_hdd'
    SSD = 'local_ssd'
