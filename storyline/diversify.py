#!/usr/bin/env python

import os

from trackutil.confutil import get_config
from trackutil.logger import INFO
from trackutil.pathutil import mkdir, get_datafiles_in_dir
from trackutil.pathutil import get_storyline_root
from trackutil.pathutil import get_storyline_module_dir
from trackutil.pathutil import get_ts_int_in_dir
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


def diversify_new(ts, cfg):
    INFO('[Diversify] {}'.format(ts))
    inputdir = get_storyline_module_dir(cfg, 'preprocess')
    outputdir = get_storyline_module_dir(cfg, 'diversify')
    mkdir(outputdir)
    sticky_pairs = gen_sticky_pairs(ts, cfg)
    INFO(len(sticky_pairs))
    tweets = jsonload(os.path.join(inputdir, '{}.json'.format(ts)))
    tweets_cleaned = []
    for tweet in tweets:
        kwpairs = tweet['kwpairs']
        for skwp in sticky_pairs:
            if tuple(sorted(skwp)) in kwpairs:
                kwpairs.remove(tuple(sorted(skwp)))
        if len(kwpairs) > 0:
            tweet['kwpairs'] = kwpairs
            tweets_cleaned.append(tweet)
        else:
            INFO('Removed one tweet by diversify')
    jsondump(tweets_cleaned, os.path.join(outputdir, '{}.json'.format(ts)))


def gen_sticky_pairs(ts, cfg):
    '''
    This function get all redundant kw pairs for tweets.
    The definition of sticky pairs is:
        1. The kw pair must be adjacent
        2. When they appear, they always appear together
        3. The appeared order is always the same.
        There *always* is defined in a threshold fashion
    '''
    inputdir = get_storyline_module_dir(cfg, 'preprocess')
    root = get_storyline_root(cfg)
    metadir = os.path.join(root, cfg['storyline']['diversify']['metadir'])
    mkdir(metadir)
    ts_list = get_ts_int_in_dir(inputdir)
    cur_idx = ts_list.index(ts)
    lookback_cnt = cfg['storyline']['diversify']['lookback']
    kw2tweets = {}
    adjacent_kwp_list = []
    sticky_kwp_list = []
    tweets = jsonload(os.path.join(inputdir, '{}.json'.format(ts)))
    for tweet in tweets:
        keywords = tweet['keywords']
        for kw in keywords:
            if kw not in kw2tweets:
                kw2tweets[kw] = []
            kw2tweets[kw].append(tweet)
        for i in range(len(keywords) - 1):
            adjacent_kwp_list.append((keywords[i], keywords[i + 1]))
    INFO(len(adjacent_kwp_list))
    jsondump(kw2tweets, os.path.join(metadir, '{}.kw2tweets.json'.format(ts)))
    jsondump(adjacent_kwp_list,
             os.path.join(metadir, '{}.akwp.json'.format(ts)))

    for t in ts_list[:cur_idx][-lookback_cnt:]:
        INFO('Processing historical ts {}, cur ts is {}'.format(t, ts))
        tmp_kw2tweets = jsonload(
            os.path.join(metadir, '{}.kw2tweets.json'.format(t)))
        tmp_adjacent_kwp_list = jsonload(
            os.path.join(metadir, '{}.akwp.json'.format(t)))
        for kw in tmp_kw2tweets:
            if kw in kw2tweets:
                kw2tweets[kw].extend(tmp_kw2tweets[kw])
            else:
                kw2tweets[kw] = tmp_kw2tweets[kw]
        adjacent_kwp_list.extend(tmp_adjacent_kwp_list)

    kw_removal_thresh = cfg['storyline']['diversify']['kw_remove_thresh']
    #  for t in ts_list[:cur_idx + 1][-lookback_cnt:]:
    #      fn = os.path.join(inputdir, '{}.json'.format(t))
    #      tweets = jsonload(fn)
    #      for tweet in tweets:
    #          keywords = tweet['keywords']
    #          for kw in keywords:
    #              if kw not in kw2tweets:
    #                  kw2tweets[kw] = []
    #              kw2tweets[kw].append(tweet)
    #          for i in range(len(keywords) - 1):
    #              adjacent_kwp_list.append((keywords[i], keywords[i + 1]))
    for akwp in adjacent_kwp_list:
        intersection = []
        (kw1, kw2) = akwp
        tidset1 = set([t['id'] for t in kw2tweets[kw1]])
        tidset2 = set([t['id'] for t in kw2tweets[kw2]])
        tidintersection = tidset1.intersection(tidset2)
        for tweet in kw2tweets[kw1]:
            if tweet['id'] in tidintersection:
                intersection.append(tweet)
        cnt = 0
        for tweet in intersection:
            keywords = tweet['keywords']
            if kw1 not in keywords:
                continue
            if kw2 not in keywords:
                continue
            idx1 = keywords.index(kw1)
            idx2 = keywords.index(kw2)
            if idx2 - idx1 == 1:
                cnt += 1
        if cnt >= kw_removal_thresh * len(intersection):
            sticky_kwp_list.append(akwp)
    return sticky_kwp_list


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
