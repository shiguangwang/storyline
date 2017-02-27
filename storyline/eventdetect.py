#!/usr/bin/env python

import os

from trackutil.pathutil import get_timestamps_in_dir,\
    get_datafiles_in_dir, mkdir
from trackutil.pathutil import get_ts_int_in_dir
from trackutil.pathutil import get_storyline_module_dir
from trackutil.pathutil import get_storyline_root
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
    metadir = os.path.join(root, cfg['storyline']['detect']['metadatadir'])
    thresh = cfg['storyline']['detect']['ig_thresh']
    tslist = get_timestamps_in_dir(inputdir)
    processed = 0
    total = len(tslist)
    for i in range(total):
        if i == 0:
            pre_ts = -1
        else:
            pre_ts = tslist[i - 1]
        ts = tslist[i]
        pre_top_kwp_list = get_pre_top_ig_kwp(metadir, pre_ts)
        get_top_ig_buckets(
            inputdir, outputdir, ts, thresh, pre_top_kwp_list, metadir)
        processed += 1
        INFO('processed {}/{}'.format(processed, total))


def eventdetect(ts, cfg=None):
    INFO('[High IG Keyword Pair Detect] {}'.format(ts))
    if cfg is None:
        cfg = get_config()
    root = get_storyline_root(cfg)
    inputdir = get_storyline_module_dir(cfg, 'bucketize')
    outputdir = get_storyline_module_dir(cfg, 'detect')
    metadir = os.path.join(root, cfg['storyline']['detect']['metadatadir'])
    thresh = cfg['storyline']['detect']['ig_thresh']
    mkdir(outputdir)
    tslist = get_ts_int_in_dir(inputdir)
    cur_idx = tslist.index(ts)
    if cur_idx == 0:
        pre_ts = -1
    else:
        pre_ts = tslist[cur_idx - 1]
    pre_top_kwp_list = get_pre_top_ig_kwp(metadir, pre_ts)
    get_top_ig_buckets(
        inputdir, outputdir, ts, thresh, pre_top_kwp_list, metadir)


def get_pre_top_ig_kwp(datadir, pre_ts):
    '''
    Get the top IG keyword pairs of the (previous) timestamp.
    '''
    if pre_ts < 0:
        return []
    mkdir(datadir)
    fn = '{}.json'.format(pre_ts)
    kwp_list = jsonload(os.path.join(datadir, fn))
    if kwp_list is None:
        kwp_list = []
    return kwp_list


def get_top_ig_buckets(input_dir, outputdir, ts, thresh, pre_top_kwp_list,
                       metadir):
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
    pre_top_kwp_list = set(pre_top_kwp_list)
    stat_inherited_num = 0
    stat_skipped_pre_num = 0
    for kwp in cur_buckets:
        # process the inherited high IG keyword pairs from the pre window
        if kwp in pre_top_kwp_list:
            if kwp not in pre_buckets:
                pre_len = 0
            else:
                pre_len = len(pre_buckets[kwp])
            cur_len = len(cur_buckets[kwp])
            if cur_len > pre_len / 2.0:
                result[kwp] = {}
                result[kwp]['tweets'] = cur_buckets[kwp]
                result[kwp]['ig'] = -1000.0
                result[kwp]['igparam'] = []
                stat_inherited_num += 1
            else:
                #  INFO('Found a kwp {} that we stop tracking'.format(kwp))
                stat_skipped_pre_num += 1
            continue  # we just continue to process next kwp
        assert kwp not in pre_top_kwp_list
        # process the non-inherited keyword pairs
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
    INFO('Inherited {} kwp, skipped {} kwp from pre window'.format(
        stat_inherited_num, stat_skipped_pre_num))
    fn = '{}.json'.format(ts)
    jsondump(result, os.path.join(outputdir, fn))
    # dump the high IG kwp for next window
    jsondump(result.keys(), os.path.join(metadir, fn))


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
    ts = str(ts)
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
