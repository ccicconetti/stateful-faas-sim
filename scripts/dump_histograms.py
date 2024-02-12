#!/usr/bin/env python3

# convert the empiric distribution from data/distributions in this repo:
#
# https://github.com/All-less/trace-generator
#
# into text tile binned histograms
#
# works with Python 3.5.2 and scipy 1.4.1

import pickle

dict_files = [
    "cpl_dist.pkl",
    "level_dist.pkl",
    ]
files=[
    "instance_cpu_dist.pkl",
    "instance_duration_dist.pkl",
    "instance_mem_dist.pkl",
    "instance_num_dist.pkl",
    "job_interval_dist.pkl",
    "task_cpu_dist.pkl",
    "task_duration_dist.pkl",
    "task_mem_dist.pkl",
    "task_num_dist.pkl",
    ]

for file in dict_files+files:
    with open(file, "rb") as f:
        rvs = pickle.load(f)
        outfilename=file.replace(".pkl", "")
        if file in dict_files:
            for k, v in rvs.items():
                print("{} {} {} {}".format(file, k, len(v._histogram[0]), (len(v._histogram[1]))))
                assert len(v._histogram[0]) == (len(v._histogram[1])-1)
                with open("{}-{}.dat".format(outfilename, k), "w") as outfile:
                    for i in range(len(v._histogram[0])):
                        outfile.write("{} {}\n".format(v._histogram[0][i], v._histogram[1][i+1]))
        else:
            print("{} {} {}".format(file, len(rvs._histogram[0]), (len(rvs._histogram[1]))))
            assert len(rvs._histogram[0]) == (len(rvs._histogram[1])-1)
            with open("{}.dat".format(outfilename), "w") as outfile:
                for i in range(len(rvs._histogram[0])):
                    outfile.write("{} {}\n".format(rvs._histogram[0][i], rvs._histogram[1][i+1]))
