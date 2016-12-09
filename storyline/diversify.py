#!/usr/bin/env python

import os

from trackutil.confutil import get_config
from trackutil.logger import INFO
from trackutil.pathutil import mkdir, get_datafiles_in_dir
from trackutil.ioutil import jsonload, jsondump


def main():
    cfg = get_config()
    root = cfg['data']['outdir']
    root = os.path.join(root, cfg['storyline']['datadir'])
    inputdir = os.path.join(root, cfg['storyline']['preprocess']['datadir'])
    outputdir = os.path.join(root, cfg['storyline']['diversify']['datadir'])
    thresh = cfg['storyline']['diversify']['kw_remove_thresh']
    redundant_pairs = get_redundant_pairs(inputdir, thresh)
    redundant_pairs_fn = cfg['storyline']['diversify']['redundant_pairs_fn']
    mkdir(outputdir)
    INFO(len(redundant_pairs))
    jsondump(redundant_pairs, os.path.join(outputdir, redundant_pairs_fn))
    diversify(inputdir, outputdir, set(redundant_pairs.keys()))


def get_redundant_pairs(inputdir, thresh):
    '''
    This function get all redundant kw pairs for the tweets of the inputdir.

    Here redundant pair is defined as follows:
        1. The kw pair must be adjacent after preprocessing.
        2. The size of the size of the tweets containing the kw pairs must be
        at least greater than some portion of the intersection between the sets
        of tweets contains either of the two keywords. The portion is defined
        in the thresh parameter.
    '''
    fn_list = get_datafiles_in_dir(inputdir)
    token_dic = {}
    pair_dic = {}
    processed = 0
    tobe_processed = len(fn_list)
    for fn in fn_list:
        tweets = jsonload(os.path.join(inputdir, fn))
        for tweet in tweets:
            for token in tweet['keywords']:
                if token not in token_dic:
                    token_dic[token] = set([])
                token_dic[token].add(tweet['id'])
            for i in range(len(tweet['keywords']) - 1):
                pair = [tweet['keywords'][i], tweet['keywords'][i + 1]]
                pair.sort()
                pair = '__'.join(pair)
                if pair not in pair_dic:
                    pair_dic[pair] = set([])
                pair_dic[pair].add(tweet['id'])
        processed += 1
        if processed % 100 == 0:
            INFO("processed {0}/{1}".format(processed, tobe_processed))
    redundant_pairs = {}
    processed = 0
    tobe_processed = len(pair_dic)
    for pair in pair_dic:
        t1, t2 = pair.split('__')
        total = len(token_dic[t1].intersection(token_dic[t2]))
        cooccur = len(pair_dic[pair])
        if cooccur * 1.0 / total >= thresh:
            redundant_pairs[pair] = [cooccur, total]
        processed += 1
        if processed % 5000 == 0:
            INFO("processed {0}/{1}".format(processed, tobe_processed))
    return redundant_pairs


def diversify(inputdir, outputdir, redundant_pairs):
    '''
    This function tries to remove the redundant kw pairs from all the tweets
    of the inputdir, and output the results to the outputdir.
    The redundant kw pairs are defined in the redundant_pairs parameter.
    '''
    mkdir(outputdir)
    fn_list = get_datafiles_in_dir(inputdir)
    processed = 0
    total = len(fn_list)
    for fn in fn_list:
        tweets = jsonload(os.path.join(inputdir, fn))
        cleaned = diversity_tweets(tweets, redundant_pairs)
        jsondump(cleaned, os.path.join(outputdir, fn))
        processed += 1
        if int(processed * 100.0 / total) % 5 == 0:
            INFO("processed {0}/{1}".format(processed, total))


def diversity_tweets(tweets, redundant_pairs):
    '''
    This function removes the redundant kw pairs for each tweet in a list of
    tweets.
    It also removes the tweet if it only contains just one kw pair. This is
    from some practical observations.
    '''
    cleaned = []
    for tweet in tweets:
        kwp_set = set([])
        for kwp in tweet['kwpairs']:
            kwp.sort()
            kwp_set.add('__'.join(kwp))
        kwp_set = kwp_set.difference(redundant_pairs)
        if len(kwp_set) > 0:
            tweet['kwpairs'] = []
            for kwp in kwp_set:
                tweet['kwpairs'].append(kwp.split('__'))
            if len(tweet['kwpairs']) > 1:
                cleaned.append(tweet)
    return cleaned


if __name__ == '__main__':
    main()
