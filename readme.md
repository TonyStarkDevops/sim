


This is a custom simulator made for simulating execution of HPC jobs on different partitions with custom algorithms.







# Input



Your input for job traces is a `csv` file with following columns:



- `id`: *int*

- `submission_time`: *int*

- `execution_time_ssd`: *int*

- `execution_time_hdd`: *int*

- `execution_time_cache`: *int*

- `partition`: *str*, options: {`local_ssd`, `central_hdd`, `cache_with_hdd`}

- `application_name`: *str*

- `dataset_name`: *str* (optional)



	- *Note: filling the field with `nan` is equal to not providing the name and will result in identifying the job as **NOT** `io_intensive`.*



- `read_io`: *int/float* (optional)





# Configs & Command Line Arguments



## Configs



There is `config.txt` file where different configs can be set:



- `scheduling_version`: an int that determines the scheduler version

- `SSD_BW`: *int/float*, bandwidth of SSD

- `HDD_BW`: *int/float*, bandwidth of HDD

- `SSD_BLOCK_SIZE`: *int/float*, block size of SSD

- `HDD_BLOCK_SIZE`: *int/float*, block size of HDD

- `SSD_IOPS`: *int/float*, SSD IOPS

- `HDD_IOPS`: *int/float*, HDD IOPS

- `HDD_node_count`: *int*, number of nodes in HDD partition

- `SSD_node_count`: *int*, number of nodes in SSD partition

- `Cache_node_count`: *int*, number of nodes in Cache partition



# Command Line Arguments



These arguements are used for reading input files with following format easier:

`{NUMBER OF JOBS}jobs_user_partitioning_{USER PARTITIONING}_{HIT RATE}_hit_{WORKLOAD NUMBER}`



- `-w`, `--workload_number`: *int*

- `-n`, `--number_of_jobs`: *int*

- `-v`, `--version`: *int*, this determines the scheduling version
	- For floating versions like **2.5** or **2.1** use **25** and **21**.

- `-uv`, `--user_partitioning_version`: *str*, currently there are 4 options: `rich`, `eco`, `nocache`, `onlycache`, `mix`
	- If `-uv` is to `mix`, then two more arguments must be specifies. `--hdd_rate` and `--cache_rate`. Both have to be a float in [0, 1]. This would indicate that `--hdd_rate`% of users select HDD partition and `--cache_rate`% of users select CACHE partition. Note that these arguments does not change anything in input and output and is only used for reading/writing to **filename**.

- `-hr`, `--hit_rate`: *str*, currently there are 3 options: `high`, `mid`, `low`

- `-bs`, `--batch_size`: *int*, this argument is used in case of multi job (or batch scheduling). e.g. when this parameter is set to 5, minisim would take (at most) 5 jobs that are submitted in the same time, and make decision for them. Currently, This argument is only used in scheduler version 4.1.

- `-fr`, or `--famous_rate`: *float*
	- indicates the file with the determined percent of jobs with famous dataset. e.g. `100jobs_user_partitioning_rich_high_hit_0.3_famous_1.csv`, this means that in the file 30% of jobs have famous dataset.
	- Options: `0.1`, `0.2`, `0.3`

 *if `-w` argument is not set, `minisim` would read from a predefined file. This file is hardcoded and you can change it in the source code.*

# How to run:



In `minisim.py` Specify the address of your job trace input file (if you are not using arguments for reading inputs). e.g.:


    self.jobs = parse_csv(YOUR INPUT FILE ADDRESS)
    self.jobs = parse_csv('./inputs/some_real_jobs.csv')


Then run `python3 minisim.py`. The results of simulation will be saved in `out_jobs_{scheduling_version}.csv`.



Also this is a full example of using command line arguments to run MiniSim:

`python3 minisim.py -n 120 -w 13 -uv 2 -v 4 -hr 0.7`

which means MiniSim will read from the following input file: `120jobs_user_partitioning2_0.7_hit_13.csv` with **scheduling version** of **4**.

 ## Scheduling Versions
 ### Version 0 (SAN HDD)
 In this version all of nodes are converted to **HDD** (e.g. if you specify 3 **Cache** 3 **SSD** 3 **HDD** the platform in this version would have 9 **HDD**) and all of the jobs are sent to **HDDs**.
 ### Version 1 (User Partitioning)
 in this version all of the jobs are scheduled based on the what user specified in the input file.

 ### Version 1.1 (FCFS)
 first-come, first-served

  ### Version Hit-aware (2.1)
  - High hit jobs are sent to **Cache**
  - Low hit jobs are sent to **HDD**

 ### Version Hit & data-aware (2.5)
 - if dataset is *famous*:
	 - **SSD**
 - else:
	- High hit jobs are sent to **Cache**
    - Low hit jobs are sent to **HDD**
 
 ### Version Hit & data & load-aware (2.9)
 - if dataset is *famous* and ssd is free:
	 - **SSD**
 - elif application is high hit:
	- High hit jobs are sent to **Cache**
    - Low hit jobs are sent to **HDD**

 ### Version Time-aware (ignore ssd) 4
In this version, after submission of each job, the job will sent to the partition with earliest finish time and if dataset is not famous will not assign to **SSD**.

### Version Time & Future-aware (ignore ssd) 4.1
This version is a multi job scheduler. You can specify a *bath size* argument that determines the maximum number of jobs that will be scheduled together. Note that jobs that are submitted in the same time may be scheduled together. For example, consider a workload with 200 jobs that are all submitted at time 1 and batch size is 10. Then scheduler will find the scheduling that leads to least amount of turnaround time. Then scheduler would do the same for the next 10 jobs. if dataset is not famous will not assign to **SSD**.

### Version Time & Future-aware (heuristic) 4.2
The algorithm mirrors version 4.1 in its structure, yet it employs a heuristic approach. Initially, it computes permutations for resources, selecting the top 100 states. Following this, it determines the permutation for the sequence/order of jobs.

# Scripts for Input Scaling

- 3 scripts are provided to scale inputs. They both read from a **base input file** (which is currently `15jobs1.csv` and can be changed by user), and they scaleup that. These scripts are in `inputs` folder.
- scale_pro.py, run `python scale_pro.py --help` 
# Scripts for Running all algorithm
this script run all algorithm on all files in inputs by running **bash runner.sh**

# Scripts for drawing
this script draw the all form of visualization for all file in output file by running **bash draw.sh** 


## `scale_input.py`

This script simply replicate and randomize the base input file.

**Arguments**:

- `-n` or `--number_of_jobs`: *int*

	- This parameter determines the number of jobs in the scaled up output. e.g. when the base file contains 15 jobs and this parameter is set to 120, the script replicated the input 8 time to produce 120 jobs. Note that the distribution of jobs will not be changed. Also the parameter must be divided by the number of jobs in in the base input file.

- `-w` or `--workload_number`: *int*

	- This parameter can be used to differentiate between generated files. This number would be attached to then end of the output file. e.g. `120jobs17.csv` where 17 is workload number.



### How to run:

Example:

`python3 scale_up.py -n 240 -w 7`

This would read from your base input file and generate a new input file with 240 jobs named `240jobs7.csv`.




## `create_cpu_intensive_workload.py`



This script can scale up input file with more options. Same as the previous script, it reads from a base input file and scale up it by desired parameters.

**Arguments:**

- `-n` or `--number_of_jobs`: *int*

	- This parameter determines the number of jobs in the scaled up output. e.g. when the base file contains 15 jobs and this parameter is set to 120, the script replicated the input 8 time to produce 120 jobs. Note that the distribution of jobs will not be changed. Also the parameter must be divided by the number of jobs in in the base input file.

- `-w` or `--workload_number`: *int*

	- This parameter can be used to differentiate between generated files. This number would be attached to then end of the output file. e.g. `120jobs17.csv` where 17 is workload number.

- `-uv` or `--user_partitioning_version`: *int*
	- options: `rich`, `eco`, `nocache`, `onlycache`, `mix`

	- Indicates the version of user partitioning. There are some predefined user partitioning versions and users can add they desired version in the script (For this, users should modify `set_user_partitioning` function).

	- If `-uv` is to `mix`, then two more arguments must be specifies. `--hdd_rate` and `--cache_rate`. Both have to be a float in [0, 1]. This would indicate that `--hdd_rate`% of users select HDD partition and `--cache_rate`% of users select CACHE partition.

- `-cr` or `--cpu_rate`: *float*

	- Indicates the distribution of CPU-intensive jobs in the generated output. For instance, when the parameter is set to `0.7`, it means 70% of jobs in the output would be CPU-intensive and others are IO-Intensive.
- `-hr`, or `--hit_rate`: *str*
	- Only a label that would be attached to the output file. e.g. `100jobs_user_partitioning_rich_high_hit_1.csv`
	- Options: `high`, `low`, `mid`

- `-fr`, or `--famous_rate`: *float*
	- Only a label that would be attached to the output file. e.g. `100jobs_user_partitioning_rich_high_hit_0.3_famous_1.csv`, this means in the generated file 30% of jobs have famous dataset.
	- Options: `0.1`, `0.2`, `0.3`



### How to run:

Example 1:

`python3 create_cpu_intensive_workload.py -n 240 -w 7 -uv nocache -hr high`

This would read from base input file and generate a new input file with 240 jobs named `240jobs_user_partitioning_nocache_high_hit_7.csv`.

Example 2:

`python3 create_cpu_intensive_workload.py -n 200 -w 7 -uv mix --cache_rate 0.7 --hdd_rate 0.3 -hr high`

This would read from base input file and generate a new input file with 240 jobs named `200jobs_user_partitioning_mix_0.3HDD_0.7CACHE_high_hit_7.csv`.


## `mix_hit_rate`
This script helps you to create workloads with varying hit rates. First you should create 2 workloads with equal number of jobs, user partitioning and workload number; one must be high hit, another one must be low hit. then by specifying the hit rate, the script will generate new workload with your desire hit rate.

**Arguments:**
- `-n` or `--number_of_jobs`: *int*
- `-w` or `--workload_number`: *int*
- `-uv` or `--user_partitioning_version`: *int*
	- options: `rich`, `eco`, `nocache`, `onlycache`
	- Indicates the version of user partitioning. There are some predefined user partitioning versions and users can add they desired version in the script (For this, users should modify `set_user_partitioning` function).
	- If `-uv` is to `mix`, then two more arguments must be specifies. `--hdd_rate` and `--cache_rate`. Both have to be a float in [0, 1]. This would indicate that `--hdd_rate`% of users select HDD partition and `--cache_rate`% of users select CACHE partition. Note that these arguments does not change anything in input and output and is only used for reading/writing to **filename**.

- `-hr`, or `--hit_rate`: *float*
- `-fr`, or `--famous_rate`: *float*
	- Only a label that would be attached to the output file. e.g. `100jobs_user_partitioning_rich_high_hit_0.3_famous_1.csv`, this means in the generated file 30% of jobs have famous dataset.
	- Options: `0.1`, `0.2`, `0.3`

**Example 1:**

    python3 mix_hit_rate.py -n 50 -w 1 -uv onlycache -hr 0.7
this script would read from the 2 following input files:

    INPUT_FILE_NAME_HIGH_HIT  =  f'{NUMBER_OF_JOBS}jobs_user_partitioning_{USER_PARTITIONING_VERSION}_high_hit_{WORKLOAD_NUMBER}.csv'

    INPUT_FILE_NAME_LOW_HIT  =  f'{NUMBER_OF_JOBS}jobs_user_partitioning_{USER_PARTITIONING_VERSION}_low_hit_{WORKLOAD_NUMBER}.csv'

Then generate output (`50jobs_user_partitioning_onlycache_0.7_hit_1`) where 70% of jobs are high hit and 30% are low hit.
You can change the order of high hit jobs and low hit jobs (in the output) by modifying the source code where the jobs are concatenated:

    df = pd.concat([df_low_hit, df_high_hit]).reset_index()

**Example 2:**

	python3 mix_hit_rate.py -n 50 -w 1 -uv mix --cache_rate 0.7 --hdd_rate 0.3 -hr 0.7

generates output (`50jobs_user_partitioning_mix_0.3HDD_0.7CACHE_0.7_hit_1`) where 70% of jobs are high hit and 30% are low hit.

# Some Notes

1. hit rate of jobs (whether they are high hit or low hit) is determined by their applications. If their application is specified as **high hit** the job would be so and the same holds for **low hit** jobs. You can find the list of applications and their attributes in the `application_manager.py`. Ath the moment if the `hit_rate` of the application is less than `0.5` the application will considered as **high hit** and viseversa.
