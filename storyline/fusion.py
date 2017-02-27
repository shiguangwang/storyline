import os

from trackutil.alg import jaccard_similarity_var
from trackutil.ioutil import jsonload, jsondump
from trackutil.pathutil import mkdir, get_datafiles_in_dir
from trackutil.pathutil import get_storyline_root
from trackutil.pathutil import get_storyline_module_dir
from trackutil.confutil import get_config
from trackutil.logger import INFO
from trackutil.textutil import tokenize_text_wrapper


def match_buckets(tbucket, ibucket):
    '''
    Check whether the tbucket and ibucket are referring to the same event.
    Return: True if match, False if not.
    '''
    if len(tbucket['twitter']) < len(ibucket['twitter']):
        small_tweets, big_tweets = tbucket['twitter'], ibucket['twitter']
    else:
        small_tweets, big_tweets = ibucket['twitter'], tbucket['twitter']
    matching_cnt = 0
    big_tids = set([t[0] for t in big_tweets])
    for t in small_tweets:
        if t[0] in big_tids:
            matching_cnt += 1
            return True  # a very aggresive way to merge..
            continue
        t_tokens = tokenize_text_wrapper(t[1])
        for r in big_tweets:
            r_tokens = tokenize_text_wrapper(r[1])
            similarity = jaccard_similarity_var(t_tokens, r_tokens)
            if similarity >= 0.7:
                matching_cnt += 1
                break
    if matching_cnt * 1.0 / len(small_tweets) >= 0.5:
        return True
    return False


def fusion_files(tfile, ifile):
    tbuckets = jsonload(tfile)
    ibuckets = jsonload(ifile)
    fbuckets = []

    if tbuckets is None or ibuckets is None:
        if tbuckets is not None:
            return tbuckets
        if ibuckets is not None:
            return ibuckets
        return fbuckets

    twrapper = {str(i): tbuckets[i] for i in range(len(tbuckets))}
    iwrapper = {str(i): ibuckets[i] for i in range(len(ibuckets))}

    itmatch = {}
    timatch = {}

    for i in iwrapper:
        ibucket = iwrapper[i]
        for j in twrapper:
            tbucket = twrapper[j]
            if match_buckets(tbucket, ibucket):
                if i not in itmatch:
                    itmatch[i] = set([])
                itmatch[i].add(j)
                if j not in timatch:
                    timatch[j] = set([])
                timatch[j].add(i)
                INFO('Found a match, ibucket: {}, tbucket: {}'.format(i, j))

    for key in itmatch:
        if len(itmatch[key]) > 1:
            backedup = [t for t in itmatch[key]]
            merged_tkey = '_'.join(itmatch[key])
            tbucket_list = []
            for k in itmatch[key]:
                tbucket_list.append(twrapper[k])
            merged_tbucket = fusion_tbuckets(tbucket_list)
            timatch_val = set([])
            for tkey in itmatch[key]:
                for ikey in timatch[tkey]:
                    itmatch[ikey] = itmatch[ikey].difference(backedup)
                    itmatch[ikey].add(merged_tkey)
                twrapper.pop(tkey)
                timatch_val = timatch_val.union(timatch.pop(tkey))
            twrapper[merged_tkey] = merged_tbucket
            timatch['_'.join(backedup)] = timatch_val

    for key in timatch:
        if len(timatch[key]) > 1:
            backedup = [t for t in timatch[key]]
            merged_ikey = '_'.join(timatch[key])
            ibucket_list = []
            for k in timatch[key]:
                ibucket_list.append(iwrapper[k])
            merged_ibucket = fusion_ibuckets(ibucket_list)
            itmatch_val = set([])
            for ikey in timatch[key]:
                for tkey in itmatch[ikey]:
                    timatch[tkey] = timatch[tkey].difference(backedup)
                    timatch[tkey].add(merged_ikey)
                iwrapper.pop(ikey)
                itmatch_val = itmatch_val.union(itmatch.pop(ikey))
            iwrapper[merged_ikey] = merged_ibucket
            itmatch['_'.join(backedup)] = itmatch_val

    INFO('itmatch size is {}, timatch size is {}'.format(
        len(itmatch), len(timatch)))
    assert len(itmatch) == len(timatch)

    for ikey in itmatch:
        ibucket = iwrapper[ikey]
        assert len(itmatch[ikey]) == 1
        tkey = itmatch[ikey].pop()
        tbucket = twrapper[tkey]
        fbucket = fusion_tibuckets(tbucket, ibucket)
        iwrapper.pop(ikey)
        twrapper.pop(tkey)
        fbuckets.append(fbucket)

    buckets = []
    buckets.extend(fbuckets)
    buckets.extend([twrapper[k] for k in twrapper])
    buckets.extend([iwrapper[k] for k in iwrapper])
    return buckets


def fusion_tbuckets(tbucket_list):
    bucket = {
        'signature': '__'.join([b['signature'] for b in tbucket_list]),
        't_bucket': True,
        'i_bucket': False,
        'instagram': [],
        'twitter': [],
    }
    tids = set([])
    for b in tbucket_list:
        for t in b['twitter']:
            if t[0] not in tids:
                tids.add(t[0])
                bucket['twitter'].append(t)
    return bucket


def fusion_ibuckets(ibucket_list):
    bucket = {
        'signature': '',
        't_bucket': False,
        'i_bucket': True,
        'twitter': [],
        'instagram': [],
    }
    tids = set([])
    for b in ibucket_list:
        for t in b['twitter']:
            if t[0] not in tids:
                tids.add(t[0])
                bucket['twitter'].append(t)
        bucket['instagram'].extend(b['instagram'])
    return bucket


def fusion_tibuckets(tbucket, ibucket):
    bucket = {
        'signature': tbucket['signature'],
        't_bucket': True,
        'i_bucket': True,
        'instagram': ibucket['instagram'],
        'twitter': [],
    }
    tids = set([])
    for t in tbucket['twitter']:
        tid = t[0]
        if tid not in tids:
            tids.add(tid)
            bucket['twitter'].append(t)
    for t in ibucket['twitter']:
        tid = t[0]
        if tid not in tids:
            tids.add(tid)
            bucket['twitter'].append(t)
    return bucket


def fusion(tdir, idir, outdir):
    mkdir(outdir)
    fnlist = get_datafiles_in_dir(tdir)
    #  fnlist = ['{}.json'.format(ts) for ts in range(96, 101)]
    for fn in fnlist:
        INFO('Processing {}'.format(fn))
        tfile = os.path.join(tdir, fn)
        ifile = os.path.join(idir, fn)
        buckets = fusion_files(tfile, ifile)
        jsondump(buckets, os.path.join(outdir, fn))


def fusion_new(ts, cfg=None):
    INFO('[Fusion] {}'.format(ts))
    root = get_storyline_root(cfg)
    tdir = get_storyline_module_dir(cfg, 'reformat')
    idir = os.path.join(root, cfg['storyline']['fusion']['instagramdir'])
    outputdir = get_storyline_module_dir(cfg, 'fusion')
    tfile = os.path.join(tdir, '{}.json'.format(ts))
    ifile = os.path.join(idir, '{}.json'.format(ts))
    outputfn = os.path.join(outputdir, '{}.json'.format(ts))
    buckets = fusion_files(tfile, ifile)
    jsondump(buckets, outputfn)


def main():
    cfg = get_config()
    root = cfg['data']['outdir']
    root = os.path.join(root, cfg['storyline']['datadir'])
    tdir = os.path.join(root, cfg['storyline']['reformat']['datadir'])
    idir = cfg['storyline']['fusion']['instagramdir']
    outdir = os.path.join(root, cfg['storyline']['fusion']['datadir'])
    fusion(tdir, idir, outdir)
    #  fusion_files(os.path.join(tdir, '96.json'), os.path.join(idir, '96.json'))


if __name__ == '__main__':
    main()
