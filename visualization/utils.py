import pandas as pd


def clean_data(df: pd.DataFrame):
    if 'metadata' in df.columns:
        df.drop(['metadata'], inplace=True, axis=1)
    if 'consumed_energy' in df.columns:
        df.drop(['consumed_energy'], inplace=True, axis=1)


def get_payload_name(profiles: pd.Series):
    def get_name(profile: str):
        return profile.split('_')[0]

    return profiles.apply(lambda p: get_name(p))


def get_partition_name(partition: str):
    partitions = {
        'local_ssd': 'local_ssd',
        'central_hdd': 'SAN',
        'cache_with_hdd': 'open_CAS_HDD',
    }
    return partitions[partition]


def select_cpu_intensive_jobs(df: pd.DataFrame):
    def job_is_cpu_intensive(j: pd.Series):
        if j['dataset_name'] is None and (j['read_io'] is None or j['read_io'] < 10e9):
            return True  # CPU-intensive

        if j['dataset_name'] is not None and j['read_io'] < 10e9:
            return True  # CPU-intensive

        return False  # IO-intensive

    # CPU_INTENSIVE_APPLICATIONS = {
    #     'deepfm',
    #     'gat',
    #     'dcn',
    #     'graphsage'
    # }
    # return df[df.apply(lambda x: x['application_name'] in CPU_INTENSIVE_APPLICATIONS, axis=1)]
    return df[df.apply(lambda x: job_is_cpu_intensive(x), axis=1)]


def read_number_of_nodes():
    import configparser
    config = configparser.RawConfigParser()
    # change this line to your MINISIM folder #
    config.read('config.txt')
    ssd_count = int(config.getint('DEFAULT', 'SSD_node_count'))
    cache_count = int(config.getint('DEFAULT', 'Cache_node_count'))
    hdd_count = int(config.getint('DEFAULT', 'HDD_node_count'))
    return {
        'local_ssd': ssd_count,
        'cache_with_hdd': cache_count,
        'central_hdd': hdd_count,
        'all_nodes': ssd_count + hdd_count + cache_count,
    }


def get_position(number_of_versions: int, index: int, width: float):
    if number_of_versions == 2:
        return (-1) ** int(index) * width

    elif number_of_versions == 3:
        if index == 0:
            return -1 * width
        elif index == 1:
            return 0
        elif index == 2:
            return width

    elif number_of_versions == 4:
        if index == 0:
            return -1.5 * width
        elif index == 1:
            return -0.5 * width
        elif index == 2:
            return 0.5 * width
        elif index == 3:
            return 1.5 * width

    elif number_of_versions == 5:
        if index == 0:
            return -2 * width
        if index == 1:
            return -1 * width
        if index == 2:
            return 0
        if index == 3:
            return width
        if index == 4:
            return 2 * width

    elif number_of_versions == 6:
        if index == 0:
            return -2.5 * width
        if index == 1:
            return -1.5 * width
        if index == 2:
            return -0.5 * width
        if index == 3:
            return 0.5 * width
        if index == 4:
            return 1.5 * width
        if index == 5:
            return 2.5 * width

    elif number_of_versions == 7:
        if index == 0:
            return -3 * width
        if index == 1:
            return -2 * width
        if index == 2:
            return -1 * width
        if index == 3:
            return 0
        if index == 4:
            return 1 * width
        if index == 5:
            return 2 * width
        if index == 6:
            return 3 * width

    elif number_of_versions == 8:
        if index == 0:
            return -3.5 * width
        if index == 1:
            return -2.5 * width
        if index == 2:
            return -1.5 * width
        if index == 3:
            return - 0.5 * width
        if index == 4:
            return 0.5 * width
        if index == 5:
            return 1.5 * width
        if index == 6:
            return 2.5 * width
        if index == 7:
            return 3.5 * width


NODE_COSTS = {
    'ssd': 285,
    'hdd': 30,
    'cache': 74,
}
