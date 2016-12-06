#!/usr/bin/env python

import os

from trackutil.alg import jaccard
from trackutil.confutil import get_config
from trackutil.pathutil import mkdir, get_datafiles_in_dir
from trackutil.ioutil import jsonload, jsondump
from trackutil.logger import INFO


def main():
    cfg = get_config()
    top_k = cfg['storyline']['consolidate']['top_k']
    thresh = cfg['storyline']['consolidate']['jaccard_thresh']
    root = cfg['data']['outdir']
    root = os.path.join(root, cfg['storyline']['datadir'])
    inputdir = os.path.join(root, cfg['storyline']['detect']['datadir'])
    outputdir = os.path.join(root, cfg['storyline']['consolidate']['datadir'])
    consolidation(inputdir, outputdir, thresh, top_k)


def consolidation(inputdir, outputdir, thresh, top_k):
    mkdir(outputdir)
    fn_list = get_datafiles_in_dir(inputdir)
    processed = 0
    total = len(fn_list)
    for fn in fn_list:
        buckets = jsonload(os.path.join(inputdir, fn))
        consolidated_buckets = consolidate(buckets, thresh, top_k)
        jsondump(consolidated_buckets, os.path.join(outputdir, fn))
        processed += 1
        INFO('{0}% processed {1}/{2}'.format(
            processed * 100.0 / total, processed, total))


def consolidate(buckets, thresh, top_k):
    '''
    Consolidate buckets
    '''
    dist = cal_kwp_dist(buckets, top_k)
    uf_dic = uf(buckets, dist, thresh)
    aggr_dic = {}
    for kwp in buckets:
        root = uf_find_root(uf_dic, kwp)
        if root not in aggr_dic:
            # the value of aggr_dic is:
            # 0. kwp list
            # 1. tweet id set
            # 2. bucket value list
            aggr_dic[root] = ([], set([]), [])
        aggr_dic[root][0].extend(kwp.split('__'))
        for tweet in buckets[kwp]['tweets']:
            tid = tweet[0]
            if tid not in aggr_dic[root][1]:
                aggr_dic[root][1].add(tid)
                aggr_dic[root][2].append(tweet)
    consolidated_buckets = {}
    for k in aggr_dic:
        signature = list(set(aggr_dic[k][0]))
        signature.sort()
        signature = '__'.join(signature)
        consolidated_buckets[signature] = aggr_dic[k][2]
    return consolidated_buckets


def uf(buckets, dist, thresh):
    # The value of the uf_dic is (parent, rank)
    uf_dic = {k: [k, 1] for k in buckets}
    for item in dist:
        if item[2] > thresh:
            continue
        root0 = uf_find_root(uf_dic, item[0])
        root1 = uf_find_root(uf_dic, item[1])
        if root0 == root1:
            continue
        if uf_dic[root0][1] > uf_dic[root1][1]:
            uf_dic[root1][0] = root0
        elif uf_dic[root0][1] < uf_dic[root1][1]:
            uf_dic[root0][0] = root1
        else:
            uf_dic[root0][0] = root1
            uf_dic[root1][1] += 1
    return uf_dic


def uf_find_root(uf_dic, key):
    while uf_dic[key][0] != key:
        uf_dic[key][0] = uf_dic[uf_dic[key][0]][0]
        key = uf_dic[key][0]
    return key


def cal_kwp_dist(buckets, top_k):
    dist = []
    cmplist = [(i, j, buckets[i], buckets[j]) for i in buckets for j in buckets
               if i < j]
    for item in cmplist:
        b1 = item[2]
        b2 = item[3]
        d = jaccard(b1, b2, top_k)
        dist.append((item[0], item[1], d))
    return dist


if __name__ == '__main__':
    main()
