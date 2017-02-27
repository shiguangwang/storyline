#!/usr/bin/env python

import csv

from trackutil.ioutil import jsonload


def gen_mean_var_csv(inputfn, outputfn):
    '''
    Generate the csv where each row is in the format of (mean, variance)
    This is specifically for the unite module.
    '''
    csv_list = []
    buckets_list = jsonload(inputfn)
    # buckets format: [[[key:val], ...], ...]
    for buckets in buckets_list:
        for bucket in buckets:
            val = bucket[1]
            mark = 0
            if len(bucket) > 2:
                if bucket[2]:
                    mark = 1
                else:
                    mark = -1
            if mark == 0:
                break
            mean = val['mean']
            var = val['var']
            csv_list.append((mean, var, mark))
    with open(outputfn, 'wb') as f:
        writer = csv.writer(f)
        writer.writerows(csv_list)


if __name__ == '__main__':
    inputfn = '/home/shiguang/Projects/evtrack/data/experimental/train/__clasped.json'
    outputfn = '/home/shiguang/Projects/evtrack/data/experimental/train/__unite_mean_var.csv'
    gen_mean_var_csv(inputfn, outputfn)
