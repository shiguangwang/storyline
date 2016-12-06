#!/usr/bin/env python
import os

from trackutil.confutil import get_config
from trackutil.ioutil import jsondump, jsonload
from trackutil.pathutil import mkdir, get_datafiles_in_dir
from trackutil.logger import LOG


def bucketize():
    '''
    This function generate buckets based on the preprocessed tweets.
    Each bucket is corresponding to one kw pair.
    '''
    cfg = get_config()
    datadir = os.path.join(cfg['data']['outdir'], cfg['storyline']['datadir'])
    preprocessdir = cfg['storyline']['preprocess']['datadir']
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


def bucktize_tweet(tweets):
    ''' Generate buckets, kw pairs, from the tweets '''
    buckets = {}
    for t in tweets:
        for kwpair in t['kwpairs']:
            key = '__'.join(kwpair)
            if key not in buckets:
                buckets[key] = []
            buckets[key].append(
                [t['id'], t['text'], t['created_at'], t['keywords']])
    return buckets


if __name__ == '__main__':
    bucketize()
