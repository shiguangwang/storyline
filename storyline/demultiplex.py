#!/usr/bin/env python

import os

from trackutil.alg import jaccard_similarity_var, cal_mean_var
from trackutil.confutil import get_config
from trackutil.ioutil import jsonload, jsondump
from trackutil.logger import INFO
from trackutil.pathutil import mkdir, get_datafiles_in_dir
from trackutil.textutil import tokenize_text


def cal_mean_var_dir(inputdir, outputdir):
    '''
    We invest the distribution of a bucket containing only one event (or false
    positive) versus the bucket containing multiple events (or false positives)
    The output will be a list of tuple (mean, var).

    The data format will be:
        {
            'signature_key': [
                # tweets
                [
                    [
                        tweet_ID,
                        text,
                        creation_time,
                        keywords,
                        tokens
                    ],
                ],
                # mean_var
                (mean, var),
            ]
        }
    '''
    mkdir(outputdir)
    fn_list = get_datafiles_in_dir(inputdir)
    processed = 0
    total = len(fn_list)
    step = 6
    for i in range(0, len(fn_list), step):
        fn = fn_list[i]
        buckets = jsonload(os.path.join(inputdir, fn))
        buckets = cal_mean_var_buckets(buckets)
        jsondump(buckets, os.path.join(outputdir, fn))
        processed += step
        if processed % 10 == 0:
            INFO('{}/{}'.format(processed, total))


def cal_mean_var_buckets(buckets):
    '''
    Calculate the mean and variance for all the buckets in one window
    '''
    for k in buckets:
        tweets = buckets[k]
        mean, var = cal_mean_var_bucket(tweets)
        buckets[k] = []
        buckets[k].append(tweets)
        buckets[k].append((mean, var))
    return buckets


def append_tokens(tweet):
    cfg = get_config()
    kw_exclude_set = set([])
    search_words = set(cfg['data']['protests']['searchwords'])
    len_thresh = 1  # basically it means there is no such thresh
    text = tweet[1]
    tokens = list(set(tokenize_text(text, len_thresh, kw_exclude_set, search_words)))
    tweet.append(tokens)
    return tweet


def cal_mean_var_bucket(tweets):
    '''
    Calculate the jaccard similarity mean and variance of the tweets in one
    bucket
    '''
    for tweet in tweets:
        tweet = append_tokens(tweet)
    compare_list = [(t1, t2) for t1 in tweets for t2 in tweets
                    if t1[0] < t2[0]]
    jaccard_list = [jaccard_similarity_var(t1[4], t2[4])
                    for (t1, t2) in compare_list]
    return cal_mean_var(jaccard_list)


if __name__ == '__main__':
    cfg = get_config()
    root = cfg['data']['outdir']
    root = os.path.join(root, cfg['storyline']['datadir'])
    inputdir = os.path.join(root, cfg['storyline']['consolidate']['datadir'])
    outputdir = os.path.join(root, cfg['storyline']['demultiplex']['datadir'])
    cal_mean_var_dir(inputdir, outputdir)
