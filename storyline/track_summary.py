#!/usr/bin/env python

import os

from trackutil.ioutil import jsonload, jsondump
from trackutil.logger import INFO
from trackutil.pathutil import mkdir, get_datafiles_in_dir
from trackutil.pathutil import get_storyline_module_dir
from trackutil.pathutil import get_storyline_root
from trackutil.confutil import get_config


def summary(tracked_events, inputfn):
    '''
    tracked_events: The active (those not terminated) events list.
    cur_buckets: the buckets in the current window.

    tracked_events format:
        {
            <e_id>: {
                tid_list: [<tid>],
                <window>: <simplified bucket list>
            }
        }

    The output will be two dictions:
        1. The actively tracking events
        2. The closed events
    '''
    cur_buckets = jsonload(inputfn)
    if cur_buckets is None:
        return tracked_events, None
    _, fn = os.path.split(inputfn)
    window_id = fn.split('.')[0]
    original_eid_set = set(tracked_events.keys())
    tracking_eid_set = set([])
    for bucket in cur_buckets:
        eid = str(bucket['event_id'])
        tracking_eid_set.add(eid)
        if eid not in tracked_events:
            # this is a new event
            tracked_events[eid] = {}
            tid_list = []
            for tweet in bucket['twitter']:
                tid_list.append(tweet[0])
            tracked_events[eid]['tid_list'] = tid_list
            tracked_events[eid]['windows'] = {}
            tracked_events[eid]['windows'][window_id] = bucket
        else:
            # we are continuing tracking some event
            tid_list = tracked_events[eid]['tid_list']
            tweet_list = []
            for tweet in bucket['twitter']:
                if tweet[0] not in tid_list:
                    tid_list.append(tweet[0])
                    tweet_list.append(tweet)
            bucket['twitter'] = tweet_list
            tracked_events[eid]['tid_list'] = tid_list
            tracked_events[eid]['windows'][window_id] = bucket
    eid_set = original_eid_set.difference(tracking_eid_set)
    closed_events = {}
    for eid in eid_set:
        val = tracked_events[eid]
        closed_events[eid] = val
        tracked_events.pop(eid, None)
    return tracked_events, closed_events


def summary_wrapper(inputdir, outputdir):
    tracked_events = {}
    mkdir(outputdir)
    fnlist = get_datafiles_in_dir(inputdir)
    processed = 0
    for fn in fnlist:
        inputfn = os.path.join(inputdir, fn)
        tracked_events, closed_events = summary(tracked_events, inputfn)
        dump_closed_events(closed_events, outputdir)
        processed += 1
        if processed % 10 == 0:
            INFO('Processed {}/{}'.format(processed, len(fnlist)))
    dump_closed_events(tracked_events, outputdir)


def track_summary_new(ts, cfg=None):
    INFO('[Tracking Summary] {}'.format(ts))
    if cfg is None:
        cfg = get_config()
    inputdir = get_storyline_module_dir(cfg, 'track')
    outputdir = get_storyline_module_dir(cfg, 'track_summary')
    rootdir = get_storyline_root(cfg)
    statedir = os.path.join(
        rootdir, cfg['storyline']['track_summary']['statedir'])
    mkdir(outputdir)
    mkdir(statedir)
    state_fn = '__tracked_events.json'
    state_fn = os.path.join(statedir, state_fn)
    tracked_events = jsonload(state_fn)
    if tracked_events is None:
        tracked_events = {}
    fn = '{}.json'.format(ts)
    inputfn = os.path.join(inputdir, fn)
    tracked_events, closed_events = summary(tracked_events, inputfn)
    if closed_events is None:
        return
    dump_closed_events(closed_events, outputdir)
    jsondump(tracked_events, state_fn)


def dump_closed_events(closed_events, outputdir):
    for eid in closed_events:
        outputfn = os.path.join(outputdir, "{}.json".format(eid))
        jsondump(closed_events[eid], outputfn)
        textfn = os.path.join(outputdir, "{}.txt".format(eid))
        window_ids = closed_events[eid]['windows'].keys()
        window_ids = sorted(window_ids)
        with open(textfn, 'w') as f:
            for wid in window_ids:
                f.write("==========\n")
                f.write('{}\n'.format(wid))
                b = closed_events[eid]['windows'][wid]
                f.write('{}\n'.format(b['signature']))
                if b['i_bucket'] and b['t_bucket']:
                    bucket_type_str = 'I, T'
                    INFO('Found the IT event {}'.format(eid))
                elif b['i_bucket']:
                    bucket_type_str = 'I'
                else:
                    bucket_type_str = 'T'
                f.write('Bucket Type: {}\n'.format(bucket_type_str))
                for t in b['twitter']:
                    f.write('\t*** <{}> {}\n'.format(
                        t[0], t[1].encode('utf-8', 'ignore')))
                if len(b['instagram']) > 0:
                    f.write('\n\t\t----------Instagram----------\n')
                for i in b['instagram']:
                    f.write('\it+++ ({}) [{}]\n'.format(
                        b['instagram'].index(i),
                        i['name'].encode('utf-8', 'ignore')))
                    f.write('\t\t tags: {}\n'.format(
                        (','.join(i['tags']).encode('utf-8', 'ignore'))))
                    f.write('\t\t url: {}\n'.format(
                        i['url'].encode('utf-8', 'ignore')))
                f.write('\n')


def fusion_track_wrapper():
    cfg = get_config()
    root = cfg['data']['outdir']
    root = os.path.join(root, cfg['storyline']['datadir'])
    inputdir = os.path.join(root, cfg['storyline']['track']['datadir'])
    outputdir = os.path.join(root, cfg['storyline']['track_summary']['datadir'])
    summary_wrapper(inputdir, outputdir)


def ibucket_track_wrapper():
    inputdir = '/home/shiguang/Projects/evtrack/data/workshop/ibuckets/ibtrack'
    outputdir = '/home/shiguang/Projects/evtrack/data/workshop/ibuckets/ibtrack_summary'
    summary_wrapper(inputdir, outputdir)


if __name__ == '__main__':
    #  ibucket_track_wrapper()
    fusion_track_wrapper()
