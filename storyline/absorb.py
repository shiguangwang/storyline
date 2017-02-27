import os

from trackutil.pathutil import get_datafiles_in_dir, mkdir
from trackutil.pathutil import get_storyline_module_dir
from trackutil.ioutil import jsonload, jsondump
from trackutil.logger import INFO
from trackutil.confutil import get_config
from trackutil.textutil import bipartite_merge


def swallow(buckets, cfg):
    size = len(buckets)
    buckets = swallow_core_v2(buckets, cfg)
    iteration_cnt = 0
    while len(buckets) != size:
        iteration_cnt += 1
        size = len(buckets)
        buckets = swallow_core_v2(buckets, cfg)
        INFO('Finished {} rounds of swallow'.format(iteration_cnt))
    return buckets


def get_thresholds():
    cfg = get_config()
    similarity_thresh = cfg['storyline']['absorb']['similarity_thresh']
    absorb_thresh = cfg['storyline']['absorb']['absorb_thresh']
    return similarity_thresh, absorb_thresh


def swallow_core_v2(buckets, cfg):
    merged = {}
    large_cluster = {}
    total_merged_cnt = 0
    processed = 0
    total = len(buckets)
    similarity_thresh, absorb_thresh = get_thresholds()
    while len(buckets) > 0:
        eq_class_keys = []
        key = buckets.keys()[0]
        bucket = buckets.pop(key)
        if len(bucket) > 100:  # we only consider cluster with small size
            large_cluster[key] = bucket
            continue
        for k in merged:
            if bipartite_merge(bucket, merged[k], similarity_thresh,
                               absorb_thresh, cfg) < 0.5:
                eq_class_keys.append(k)
                total_merged_cnt += 1
        signature = key.split('__')
        tids = set([t[0] for t in bucket])
        for k in eq_class_keys:
            to_merge = merged.pop(k)
            signature.extend(k.split('__'))
            for t in to_merge:
                if t[0] not in tids:
                    tids.add(t[0])
                    bucket.append(t)
        signature = list(set(signature))
        signature.sort()
        signature = '__'.join(signature)
        merged[signature] = bucket
        processed += 1
        if processed % 10 == 0:
            INFO('Processed {}/{}'.format(processed, total))
    INFO('size of merged is {} and size of large_cluster is {}'.format(
        len(merged), len(large_cluster)))
    for key in large_cluster:
        if key not in merged:
            merged[key] = large_cluster[key]
        else:
            INFO('Absorb core detected some error that key {} in large cluster'
                 'but not in merged'.format(key))
    INFO('total_merged_cnt is {}'.format(total_merged_cnt))
    return merged


def absorb(inputfn, outputfn, cfg):
    buckets = jsonload(inputfn)
    if buckets is None:
        return
    aggregated = swallow(buckets, cfg)
    jsondump(aggregated, outputfn)


def absorb_new(ts, cfg=None):
    INFO('[Absorb Consolidation] {}'.format(ts))
    if cfg is None:
        cfg = get_config()
    inputdir = get_storyline_module_dir(cfg, 'consolidate')
    outputdir = get_storyline_module_dir(cfg, 'absorb')
    mkdir(outputdir)
    inputfn = os.path.join(inputdir, '{}.json'.format(ts))
    outputfn = os.path.join(outputdir, '{}.json'.format(ts))
    absorb(inputfn, outputfn, cfg)


def main():
    cfg = get_config()
    root = cfg['data']['outdir']
    root = os.path.join(root, cfg['storyline']['datadir'])
    inputdir = os.path.join(root, cfg['storyline']['consolidate']['datadir'])
    outputdir = os.path.join(root, cfg['storyline']['absorb']['datadir'])
    mkdir(outputdir)
    fnlist = get_datafiles_in_dir(inputdir)
    #  fnrange = range(610, 626)
    #  fnlist = ['{}.json'.format(fn) for fn in fnrange]
    for fn in fnlist:
        inputfn = os.path.join(inputdir, fn)
        outputfn = os.path.join(outputdir, fn)
        absorb(inputfn, outputfn, cfg)
    #  fn = '610.json'
    #  inputfn = os.path.join(inputdir, fn)
    #  outputfn = os.path.join(outputdir, fn)
    #  absorb(inputfn, outputfn, cfg)


if __name__ == '__main__':
    main()
