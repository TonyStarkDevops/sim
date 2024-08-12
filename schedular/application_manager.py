from typing import Dict

from minisim_config import MINISIM_CONFIG


class Application:
    def __init__(self,
                 name: str,
                 hit_rate: float,
                 default_dataset_size: float,
                 avg_execution_time: int,
                 normalized_io_rate: float,
                 is_sequential: bool) -> None:

        self.name = name
        # unit of default_dataset_size is bytes
        self.default_dataset_size = default_dataset_size
        # unit of avg_execution_time is seconds
        self.avg_execution_time = avg_execution_time
        # hit_rate and normalized_io_rate are between 0 and 1
        self.hit_rate = hit_rate
        self.normalized_io_rate = normalized_io_rate
        self.is_sequential = is_sequential

    def time_on_cache(self, total_io: int):
        return total_io / self.default_dataset_size * self.avg_execution_time

    def normalized_io_time_on_cache(self, total_io: int):
        return self.time_on_cache(total_io) * self.normalized_io_rate

    def compute_time(self, total_io: int):
        return self.time_on_cache(total_io) - self.normalized_io_time_on_cache(total_io)

    def average_io_rate_on_cache(self):
        if self.is_sequential:
            return self.hit_rate * MINISIM_CONFIG.SSD_BW + (1 - self.hit_rate) * MINISIM_CONFIG.HDD_BW
        else:
            return self.hit_rate * MINISIM_CONFIG.SSD_IOPS + (1 - self.hit_rate) * MINISIM_CONFIG.HDD_IOPS

    def io_time_on_ssd(self, total_io: int):
        if self.is_sequential:
            return self.average_io_rate_on_cache() / MINISIM_CONFIG.SSD_BW * self.normalized_io_time_on_cache(total_io)
        else:
            return self.average_io_rate_on_cache() / MINISIM_CONFIG.SSD_IOPS * self.normalized_io_time_on_cache(
                total_io)

    def io_time_on_hdd(self, total_io: int):
        if self.is_sequential:
            return self.average_io_rate_on_cache() / MINISIM_CONFIG.HDD_BW * self.normalized_io_time_on_cache(total_io)
        else:
            return self.average_io_rate_on_cache() / MINISIM_CONFIG.HDD_IOPS * self.normalized_io_time_on_cache(
                total_io)

    def total_time_on_hdd(self, total_io: int):
        return self.compute_time(total_io) + self.io_time_on_hdd(total_io)

    def total_time_on_ssd(self, total_io: int):
        return self.compute_time(total_io) + self.io_time_on_ssd(total_io)

    @property
    def is_high_hit(self):
        if self.hit_rate > 0.5:
            return True
        else:
            return False


APPLICATIONS: Dict[str, Application] = {
    'tensorflow': Application(
        name='tensorflow',
        hit_rate=0.1,
        default_dataset_size=500e9,
        avg_execution_time=120 * 60,
        normalized_io_rate=0.25,
        is_sequential=True
    ),
    'other': Application(
        name='other',
        hit_rate=0.1,
        default_dataset_size=500e9,
        avg_execution_time=120 * 60,
        normalized_io_rate=0.25,
        is_sequential=True
    ),
    'PyTorchWorker': Application(
        name='pytorch',
        hit_rate=0.7,
        default_dataset_size=500e9,
        avg_execution_time=120 * 60,
        normalized_io_rate=0.25,
        is_sequential=False,
    ),
    'worker': Application(
        name='worker',
        hit_rate=0.8,
        default_dataset_size=10e9,
        avg_execution_time=15 * 60,
        normalized_io_rate=0.25,
        is_sequential=False
    ),
    'xComputeWorker': Application(
        name='xComputeWorker',
        hit_rate=0.8,
        default_dataset_size=10e9,
        avg_execution_time=15 * 60,
        normalized_io_rate=0.25,
        is_sequential=False
    ),
    'TVMTuneMain': Application(
        name='TVMTuneMain',
        hit_rate=0.8,
        default_dataset_size=10e9,
        avg_execution_time=15 * 60,
        normalized_io_rate=0.25,
        is_sequential=False
    ),
    'OssToVolumeWorker': Application(
        name='OssToVolumeWorker',
        hit_rate=0.8,
        default_dataset_size=10e9,
        avg_execution_time=15 * 60,
        normalized_io_rate=0.25,
        is_sequential=False
    ),
    'JupyterTask': Application(
        name='JupyterTask',
        hit_rate=0.8,
        default_dataset_size=10e9,
        avg_execution_time=15 * 60,
        normalized_io_rate=0.25,
        is_sequential=False
    ),
    'BladeMain': Application(
        name='BladeMain',
        hit_rate=0.8,
        default_dataset_size=10e9,
        avg_execution_time=15 * 60,
        normalized_io_rate=0.25,
        is_sequential=False
    ),
    #### real applications
    'random_read_no_zipf': Application(
        name='random_read_no_zipf',
        hit_rate=0.01,
        default_dataset_size=200e9,
        avg_execution_time=710 * 60,
        normalized_io_rate=1,
        is_sequential=False
    ),
    'random_read_no_zipf_1.2': Application(
        name='random_read_no_zipf_1.2',
        hit_rate=0.91,
        default_dataset_size=200e9,
        avg_execution_time=493 * 60,
        normalized_io_rate=1,
        is_sequential=False
    ),
    'random_read_no_zipf_popular2': Application(
        name='random_read_no_zipf_popular2',
        hit_rate=0.18,
        default_dataset_size=200e9,
        avg_execution_time=710 * 60,
        normalized_io_rate=1,
        is_sequential=False
    ),
    'yolov7-train': Application(
        name='yolov7-train',
        hit_rate=0.7,
        default_dataset_size=200e9,
        avg_execution_time=91 * 60,
        normalized_io_rate=1,
        is_sequential=True
    ),
    'tensor_img_class1': Application(
        name='tensor_img_class1',
        hit_rate=0.7,
        default_dataset_size=200e9,
        avg_execution_time=91 * 60,
        normalized_io_rate=1,
        is_sequential=True
    ),
    'tensor_img_class2': Application(
        name='tensor_img_class2',
        hit_rate=0.7,
        default_dataset_size=200e9,
        avg_execution_time=91 * 60,
        normalized_io_rate=1,
        is_sequential=True
    ),
    'Densenet': Application(
        name='Densenet',
        hit_rate=0.7,
        default_dataset_size=200e9,
        avg_execution_time=91 * 60,
        normalized_io_rate=1,
        is_sequential=True
    ),
}


def application_has_enough_info(application_name: str) -> bool:
    if application_name in APPLICATIONS:
        return True
    return False
