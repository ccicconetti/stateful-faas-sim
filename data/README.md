# Data

Empirical distribution from `data/distributions` in https://github.com/All-less/trace-generator

## Description

Each DAG-modeled job has $N$ tasks (drawn from `task_num`), $C$ of which (drawn from `cpl[task_num]`) are in the critical path, i.e., they form a chain from source to sink.
In each level $i$ there are $L_i$ tasks (drawn from `level[cpl]`) connected with $k$ random tasks in the next level, with $k = \lceil L_{i+1}/L_i \rceil$.

Empirical distributions used:

- `job_interval` (float): interval between the arrival of consecutive jobs
- `task_num` (int): number of tasks in this DAG
- `cpl` (int): critical path length, for a given number of tasks (saturates to 35)
- `level` (int): number of siblings per level, for a given cpl (saturates to 20)
- `task_cpu` (float): task CPU requested, every 100 unit means 1 core
- `task_mem` (float): task memory requested, the fraction of 100 unit
- `task_duration` (float): task duration, in s

### Instances

Each task is mapped to a number of instances, with actual utilization metrics, according to the following distributions:

- `instance_num` (int): number of instances of the task
- `instance_cpu` (float): CPU used by a given instance of the task, in CPU%
- `instance_mem` (float): memory used by a given instance of the task, in MB
- `instance_duration` (float): duration of a given instance of the task, s

