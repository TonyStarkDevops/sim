import click
import pandas as pd


@click.command()
@click.option('--file1', help='file of jobs in output (source)')
@click.option('--file2', default=None, help='file of jobs in output (destination), if it is blank, code calculates the repartitioning of file 1 with user partition')
def calculate(file1, file2):
    if not file2:
        df = pd.read_csv(file1)
        result = df.groupby(['user_partition', 'partition']).size().reset_index(name='count')
        print(result)
        return 0
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    df = pd.DataFrame()
    df['source_partition'] = df1['partition']
    df['destination_partition'] = df2['partition']
    result = df.groupby(['destination_partition', 'source_partition']).size().reset_index(name='count')
    print(result)


if __name__ == '__main__':
    calculate()
