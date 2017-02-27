#!/usr/bin/env python

import os

from trackutil.ioutil import jsonload, jsondump
from trackutil.confutil import get_config
from trackutil.pathutil import mkdir, get_datafiles_in_dir
from trackutil.pathutil import get_storyline_module_dir
from trackutil.pathutil import get_ts_int_in_dir
from trackutil.logger import INFO
from trackutil.textutil import bipartite_merge
from trackutil.numutil import grab_event_id


def get_thresholds():
    cfg = get_config()
    tweet_thresh = cfg['storyline']['track']['similarity_thresh']
    bucket_thresh = cfg['storyline']['track']['correlation_thresh']
    return tweet_thresh, bucket_thresh


def track_core(pre_buckets, cur_buckets, cfg):
    '''
    Match the buckets in current window with previous window (with overlap).
    If match, inherite the event ID of the matched window, if not, assign
    the current bucket with a new event ID.
    '''
    tweet_thresh, bucket_thresh = get_thresholds()
    matched_buckets = []
    if pre_buckets is None:
        pre_buckets = []
    processed = 0
    for cb in cur_buckets:
        matched = False
        for pb in pre_buckets:
            if bipartite_merge(pb['twitter'], cb['twitter'], tweet_thresh,
                               bucket_thresh, cfg) < 0.5:
                cb['event_id'] = pb['event_id']
                matched = True
                break
        if not matched:
            cb['event_id'] = grab_event_id()
        matched_buckets.append(cb)
        processed += 1
        if processed % 10 == 0:
            INFO('Processed {}/{}'.format(processed, len(cur_buckets)))
    return matched_buckets


def track(inputdir, outputdir, cfg):
    mkdir(outputdir)
    fnlist = get_datafiles_in_dir(inputdir)
    for i in range(len(fnlist)):
        cur_fn = fnlist[i]
        cur_buckets = jsonload(os.path.join(inputdir, cur_fn))
        if i - 1 >= 0:
            pre_fn = fnlist[i - 1]
            pre_buckets = jsonload(os.path.join(outputdir, pre_fn))
        else:
            pre_buckets = []
        matched_buckets = track_core(pre_buckets, cur_buckets, cfg)
        jsondump(matched_buckets, os.path.join(outputdir, fnlist[i]))
        if i % 1 == 0:
            INFO('Processed {}/{}'.format(i, len(fnlist)))


def track_new(ts, input_module, output_module, cfg=None):
    INFO('[Track {}] {}'.format(input_module, ts))
    if cfg is None:
        cfg = get_config()
    inputdir = get_storyline_module_dir(cfg, input_module)
    outputdir = get_storyline_module_dir(cfg, output_module)
    mkdir(outputdir)
    tslist = get_ts_int_in_dir(inputdir)
    if ts not in tslist:
        INFO('Timestamp {} is not in tslist'.format(ts))
        return
    cur_idx = tslist.index(ts)
    cur_fn = os.path.join(inputdir, '{}.json'.format(ts))
    cur_buckets = jsonload(cur_fn)
    if cur_idx - 1 >= 0:
        pre_ts = tslist[cur_idx - 1]
        pre_fn = os.path.join(outputdir, '{}.json'.format(pre_ts))
        pre_buckets = jsonload(pre_fn)
    else:
        pre_buckets = []
    matched_buckets = track_core(pre_buckets, cur_buckets, cfg)
    jsondump(matched_buckets, os.path.join(outputdir, '{}.json'.format(ts)))


def track_wrapper():
    cfg = get_config()
    root = cfg['data']['outdir']
    root = os.path.join(root, cfg['storyline']['datadir'])
    inputdir = os.path.join(root, cfg['storyline']['fusion']['datadir'])
    outputdir = os.path.join(root, cfg['storyline']['track']['datadir'])
    track(inputdir, outputdir, cfg)


def ibucket_tracker(config_fn):
    cfg = get_config(config_fn)
    inputdir = '/home/shiguang/Projects/evtrack/data/workshop/ibuckets/ibuckets'
    outputdir = '/home/shiguang/Projects/evtrack/data/workshop/ibuckets/ibtrack'
    track(inputdir, outputdir, cfg)


if __name__ == '__main__':
    track_wrapper()
    #  ibucket_tracker()
