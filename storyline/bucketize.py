#!/usr/bin/env python
import os

from trackutil.confutil import get_config
from trackutil.ioutil import jsondump, jsonload
from trackutil.pathutil import mkdir, get_datafiles_in_dir
from trackutil.pathutil import get_storyline_module_dir
from trackutil.logger import LOG, INFO


def bucketize():
    '''
    This function generate buckets based on the preprocessed tweets.
    Each bucket is corresponding to one kw pair.
    '''
    cfg = get_config()
    datadir = os.path.join(cfg['data']['outdir'], cfg['storyline']['datadir'])
    preprocessdir = cfg['storyline']['diversify']['datadir']
    bucketizedir = cfg['storyline']['bucketize']['datadir']
    inputdir = os.path.join(datadir, preprocessdir)
    outputdir = os.path.join(datadir, bucketizedir)
    bucketize_core(inputdir, outputdir)


def bucketize_core(inputdir, outputdir):
    mkdir(outputdir)
    fnlist = get_datafiles_in_dir(inputdir)
    processed = 0
    total = len(fnlist)
    for fn in fnlist:
        tweets = jsonload(os.path.join(inputdir, fn))
        buckets = bucktize_tweet(tweets)
        jsondump(buckets, os.path.join(outputdir, fn))
        processed += 1
        if int(processed * 100.0 / total) % 5 == 0:
            LOG('INFO', '{0}/{1} processed'.format(processed, total))


def bucketize_new(ts, cfg=None):
    INFO('[Bucketize] {}'.format(ts))
    if cfg is None:
        cfg = get_config()
    input_dir = get_storyline_module_dir(cfg, 'preprocess')
    output_dir = get_storyline_module_dir(cfg, 'bucketize')
    mkdir(output_dir)
    tweets = jsonload(os.path.join(input_dir, '{}.json'.format(ts)))
    buckets = bucktize_tweet(tweets)
    kw_pop_thresh = cfg['storyline']['bucketize']['kw_pop_thresh']
    buckets_cleaned = {}
    for kwp in buckets:
        if len(buckets[kwp]) < kw_pop_thresh:
            continue
        buckets_cleaned[kwp] = buckets[kwp]
    jsondump(buckets_cleaned, os.path.join(output_dir, '{}.json'.format(ts)))


def bucktize_tweet(tweets):
    ''' Generate buckets, kw pairs, from the tweets '''
    buckets = {}
    key2tids = {}
    tidset = set([])
    for t in tweets:
        if t['id'] in tidset:
            INFO('Found anamaly')
            continue
        tidset.add(t['id'])
        for kwpair in t['kwpairs']:
            key = '__'.join(kwpair)
            if key not in key2tids:
                key2tids[key] = set([])
            if t['id'] in key2tids[key]:
                # 'Found duplicated tid'
                continue
            key2tids[key].add(t['id'])
            if key not in buckets:
                buckets[key] = []
            buckets[key].append(
                [t['id'], t['text'], 0, t['keywords']])
            # This is used in Prasanna's dataset (I-buckets)
            #  buckets[key].append(
            #      [t['id'], t['text'], t['tstamp'], t['keywords']])
    return buckets


if __name__ == '__main__':
    bucketize()
