#!/usr/bin/env python

import sys

from trackutil.confutil import get_config
from trackutil.logger import INFO
from trackutil.pathutil import get_ts_int_in_dir

from storyline.preprocess import preprocess
from storyline.bucketize import bucketize_new
from storyline.eventdetect import eventdetect
from storyline.consolidate import consolidate_new
from storyline.absorb import absorb_new
from storyline.partition import partition_new
from storyline.purify import purify_new
from storyline.summarize import summarize_new
from storyline.fusion import fusion_new
from storyline.reformat import reformat_new
from storyline.track import track_new
from storyline.track_summary import track_summary_new


def process(ts, cfg, is_fusion=False):
    preprocess(ts, cfg)
    bucketize_new(ts, cfg)
    eventdetect(ts, cfg)
    consolidate_new(ts, cfg)
    absorb_new(ts, cfg)
    partition_new(ts, cfg)
    purify_new(ts, cfg)
    reformat_new(ts, cfg)
    summarize_new(ts, 'reformat', 'summarize', cfg)
    if is_fusion:
        # TODO add the data processing of raw Instagram input to gen I-buckets
        fusion_new(ts, cfg)
        summarize_new(ts, 'fusion', 'fusion_summarize', cfg)
        track_new(ts, 'fusion', 'track', cfg)
    else:
        track_new(ts, 'reformat', 'track', cfg)
    track_summary_new(ts, cfg)


def main():
    config_fn = sys.argv[1]
    print config_fn
    cfg = get_config(config_fn)
    srcdir = cfg['data']['srcdir']
    print srcdir
    tslist = get_ts_int_in_dir(srcdir)
    processed = 0
    total = len(tslist)
    tocontinue = False
    for ts in tslist:
        if ts > 0:
            tocontinue = True
        if tslist.index(ts) > 10000 or tocontinue is True:
            process(ts, cfg)
        processed += 1
        INFO('++++++++++ Finished processing {}. ({}/{}) ++++++++++'.format(
            ts, processed, total))
        #  if processed == 200:
        #      break
    INFO('Finished.')


if __name__ == '__main__':
    main()
