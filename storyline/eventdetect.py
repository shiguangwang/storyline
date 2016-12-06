#!/usr/bin/env python

import os

from trackutil.pathutil import get_timestamps_in_dir,\
    get_datafiles_in_dir, mkdir
from trackutil.confutil import get_config
from trackutil.ioutil import jsonload, jsondump
from trackutil.logger import INFO
from trackutil.alg import ig


def main():
    cfg = get_config()
    root = cfg['data']['outdir']
    root = os.path.join(root, cfg['storyline']['datadir'])
    inputdir = os.path.join(root, cfg['storyline']['bucketize']['datadir'])
    outputdir = os.path.join(root, cfg['storyline']['detect']['datadir'])
    thresh = cfg['storyline']['detect']['ig_thresh']
    tslist = get_timestamps_in_dir(inputdir)
    processed = 0
    total = len(tslist)
    for ts in tslist:
        get_top_ig_buckets(inputdir, outputdir, ts, thresh)
        processed += 1
        if int(processed * 100.0 / total) % 5 == 0:
            INFO('processed {}/{}'.format(processed, total))


def get_top_ig_buckets(input_dir, outputdir, ts, thresh):
    '''
    Get the top Infomation Gain kw paris.
    '''
    mkdir(outputdir)
    cur_buckets, pre_buckets = get_combined_buckets(input_dir, ts)
    if cur_buckets is None:
        return
    cur_total = get_tweet_num(cur_buckets)
    pre_total = get_tweet_num(pre_buckets)
    cur_buckets = remove_unpopular_kwpair(cur_buckets)
    result = {}
    for kwp in cur_buckets:
        B = len(cur_buckets[kwp])
        D = cur_total - B
        if kwp not in pre_buckets:
            A = 0
        else:
            A = len(pre_buckets[kwp])
        C = pre_total - A
        if B < A:
            # seems the keywork pair is losing popularity
            continue
        IG = ig(A, B, C, D)
        if IG >= thresh:
            result[kwp] = {}
            result[kwp]['tweets'] = cur_buckets[kwp]
            result[kwp]['ig'] = IG
            result[kwp]['igparam'] = [A, B, C, D]
    fn = ts + '.json'
    jsondump(result, os.path.join(outputdir, fn))


def get_tweet_num(buckets):
    ''' Get the number of distinct tweets in the buckets '''
    tweets = set([])
    for key in buckets:
        for item in buckets[key]:
            tweets.add(item[0])
    return len(tweets)


def remove_unpopular_kwpair(buckets):
    '''
    Remove the unpopular kw pairs in the current window.
    '''
    cfg = get_config()
    popthresh = cfg['storyline']['detect']['kw_pop_thresh']
    buckets_cleaned = {}
    for key in buckets:
        if len(buckets[key]) < popthresh:
            continue
        buckets_cleaned[key] = buckets[key]
    return buckets_cleaned


def get_combined_buckets(input_dir, ts):
    '''
    Get the buckets in the window ending with the ts and those in the
    previous window as well. Return None, None if we do not have enough history.
    '''
    tslist = get_timestamps_in_dir(input_dir)
    idx = tslist.index(ts)
    cfg = get_config()
    windowlen = cfg['storyline']['windowlen']
    if idx + 1 < 2 * windowlen:
        INFO('Do not have enough history')
        return None, None
    cur_buckets = get_window_buckets(input_dir, ts)
    pre_buckets = get_window_buckets(input_dir, tslist[idx - windowlen])
    return cur_buckets, pre_buckets


def get_window_buckets(input_dir, ts):
    ''' Get the buckets in a window '''
    tslist = get_timestamps_in_dir(input_dir)
    fnlist = get_datafiles_in_dir(input_dir)
    cfg = get_config()
    idx = tslist.index(ts)
    windowlen = cfg['storyline']['windowlen']
    assert(idx + 1 >= windowlen)
    buckets = {}
    for i in range(idx + 1 - windowlen, idx + 1):
        b = jsonload(os.path.join(input_dir, fnlist[i]))
        for k in b:
            if k not in buckets:
                buckets[k] = []
            buckets[k].extend(b[k])
    return buckets


if __name__ == '__main__':
    main()
