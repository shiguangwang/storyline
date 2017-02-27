#!/usr/bin/env python

import csv
import os

from trackutil.pathutil import get_datafiles_in_dir
from trackutil.ioutil import jsonload

# This script collect the mean and variance output by the experimental stat
# function of the de-multiplex module and output the collection to a csv file

d = "/home/shiguang/Projects/evtrack/data/storyline/demultiplex_test4"

fn_list = get_datafiles_in_dir(d)

mean_var_list = []

for fn in fn_list:
    buckets = jsonload(os.path.join(d, fn))
    for k in buckets:
        bucket = buckets[k]
        mean_var_list.append(bucket[1])

outfn = "__mean_var.csv"

with open(os.path.join(d, outfn), 'wb') as f:
    writer = csv.writer(f)
    writer.writerows(mean_var_list)
