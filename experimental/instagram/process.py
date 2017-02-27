import os

from trackutil.ioutil import jsonload, jsondump
from trackutil.pathutil import get_datafiles_in_dir, mkdir
from storyline.summarize import summarize as event_summary


def extract_buckets_wrapper(inputdir, outputdir):
    mkdir(outputdir)
    shiftlen = 5
    fnlist = get_datafiles_in_dir(inputdir)
    for fn in fnlist:
        inputfn = os.path.join(inputdir, fn)
        buckets = extract_ibuckets(inputfn)
        shiftedfn = '{}.json'.format(int(fn.split('.')[0]) + shiftlen)
        outputfn = os.path.join(outputdir, shiftedfn)
        jsondump(buckets, os.path.join(outputdir, outputfn))


def extract_ibuckets(inputfn):
    obj = jsonload(inputfn)
    buckets = []
    events = obj['data']
    for event in events:
        bucket = reformat_ibucket(event)
        buckets.append(bucket)
    return buckets


def reformat_ibucket(ibucket):
    '''
    Reformat the original i-bucket to a general one.
    '''
    bucket = {
        'signature': '',
        't_bucket': False,
        'i_bucket': True,
        'twitter': [],
        'instagram': ibucket['instagram']
    }
    for tweet in ibucket['twitter']:
        bucket['twitter'].append([tweet[1], tweet[0]])
    return bucket


if __name__ == '__main__':
    #  orig_dir = '/home/shiguang/Projects/evtrack/data/workshop/t_buckets_24/ibuckets/output'
    #  ibuckets_dir = '/home/shiguang/Projects/evtrack/data/workshop/t_buckets_24/ibuckets/ibuckets'
    #  ibuckets_summary_dir = '/home/shiguang/Projects/evtrack/data/workshop/t_buckets_24/ibuckets/summary'
    orig_dir = '/home/shiguang/Projects/evtrack/data/workshop/ibuckets/output'
    ibuckets_dir = '/home/shiguang/Projects/evtrack/data/workshop/ibuckets/ibuckets'
    ibuckets_summary_dir = '/home/shiguang/Projects/evtrack/data/workshop/ibuckets/ibucket_summary'
    extract_buckets_wrapper(orig_dir, ibuckets_dir)
    event_summary(ibuckets_dir, ibuckets_summary_dir)
