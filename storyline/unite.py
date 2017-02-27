#!/usr/bin/env python

import os
import statistics

from trackutil.alg import jaccard_dist
from trackutil.confutil import get_config
from trackutil.pathutil import mkdir, get_datafiles_in_dir
from trackutil.ioutil import jsondump, jsonload
from trackutil.logger import INFO


def invest(inputdir, outputdir):
    '''
    We want to invest the mean and variance of each bucket to filter out the
    buckets containing more than 1 events.
    '''
    mkdir(outputdir)
    fnlist = get_datafiles_in_dir(inputdir)
    processed = 0
    total = len(fnlist)
    for fn in fnlist:
        buckets = jsonload(os.path.join(inputdir, fn))
        for key in buckets:
            bucket = buckets[key]
            mean, var = get_stats(bucket)
            bucket['mean'] = mean
            bucket['var'] = var
        jsondump(buckets, os.path.join(outputdir, fn))
        processed += 1
        if processed % 10 == 0:
            INFO("processed {0}/{1}".format(processed, total))


def get_stats(bucket):
    '''
    Return the mean and variance of the Jaccard distance between each tweet
    in the bucket
    '''
    tokens_list = [t[3] for t in bucket['tweets']]
    cnt = len(tokens_list)
    pair_list = [(i, j) for i in range(cnt) for j in range(cnt) if i < j]
    dist_list = [jaccard_dist(tokens_list[i], tokens_list[j])
                 for (i, j) in pair_list]
    mean = statistics.mean(dist_list)
    var = statistics.variance(dist_list)
    return mean, var


def invest_main():
    cfg = get_config()
    root = cfg['data']['outdir']
    root = os.path.join(root, cfg['storyline']['datadir'])
    inputdir = os.path.join(root, cfg['storyline']['detect']['datadir'])
    outputdir = os.path.join(root, cfg['storyline']['unite']['testdir'])
    invest(inputdir, outputdir)


if __name__ == '__main__':
    invest_main()
