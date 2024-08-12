import random
import click
import pandas as pd
import numpy as np


@click.command()
@click.option('--count', default=2000, help='count of jobs in output')
@click.option('--hit', default=37, help="percentage of high hit applications, 0-100")
@click.option('--famous', default=63, help="percentage of famous dataset applications, 0-100")
@click.option('--eco', default=50, help="percentage of eco users, 0-100")
@click.option('--user', default=50, help="count of users")
def scale(count, hit, famous, eco, user):
    cpu_intensive = pd.read_csv("files/cpu_intensive.csv")
    high_hit = pd.read_csv("files/high_hit.csv")
    famous_dataset = pd.read_csv("files/famous_dataset.csv")

    cpu_intensive = cpu_intensive.sample(n=count, replace=True)
    high_hit = high_hit.sample(n=count, replace=True)
    famous_dataset = famous_dataset.sample(n=count, replace=True)

    high_hit_count = int(count * hit / 100)
    famous_count = int(count * famous / 100)
    cpu_count = count - high_hit_count - famous_count

    cpu_intensive = cpu_intensive.sample(n=cpu_count, replace=True)
    high_hit = high_hit.sample(n=high_hit_count, replace=True)
    famous_dataset = famous_dataset.sample(n=famous_count, replace=True)

    df = pd.concat([cpu_intensive, high_hit, famous_dataset])
    df = df.sample(n=count)
    execution_time_avg = np.mean(
        [df["execution_time_ssd"].mean(), df["execution_time_hdd"].mean(), df["execution_time_cache"].mean()]
    )
    eco_count = int((eco / 100) * count)
    partitions = ['central_hdd'] * eco_count + ['cache_with_hdd'] * (count - eco_count)
    random.shuffle(partitions)
    lambda_value = 1000
    print(len(partitions))
    poisson_array = np.random.poisson(lambda_value, count)
    base_index = int(count * 0.01)
    minimum_value = np.sort(poisson_array)[base_index]
    poisson_array -= minimum_value
    poisson_array = np.array([1 if x <= 0 else x for x in poisson_array])
    average_value = np.mean(poisson_array)
    scale_factor = count * execution_time_avg / average_value / 300
    sorted_array = np.sort(poisson_array) * scale_factor
    df['partition'] = partitions
    df['submission_time'] = [int(x) for x in sorted_array]
    df = df.reset_index(drop=True)
    df = df.drop('Unnamed: 0', axis=1)
    df['id'] = df.index + 1
    df['user_id'] = [np.random.randint(user) for _ in range(count)]

    df.to_csv(f"generated_files/{count}_job_real_data_mix_{hit / 100}_hit_{famous / 100}_famous_poisson.csv")


if __name__ == '__main__':
    scale()
