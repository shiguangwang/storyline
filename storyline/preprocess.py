#!/usr/bin/env python

import itertools
import os

from apollo_lib.apolloutil import read_and_parse_tweets_from_file as get_tweets
from trackutil.ioutil import jsonload, jsondump
from trackutil.pathutil import mkdir, get_datafiles_in_dir
from trackutil.confutil import get_config
from trackutil.logger import INFO
from trackutil.textutil import tokenize as tok


def preprocess(input_dir, output_dir, exclude_kwlist):
    '''
    Remove the redundant and non-EN tweets (crawled duplicated) in the
    @input_dir and output to the @output_dir
    The words in the @exclude_kwlist should not be considered as a keyword
    '''
    mkdir(output_dir)
    cfg = get_config()
    twid_fn = cfg['storyline']['preprocess']['tidfilename']
    twid_fn = os.path.join(output_dir, twid_fn)
    filenames = get_datafiles_in_dir(input_dir)
    processed = 0
    total = len(filenames)
    # TODO Remove the TEMP_THRESH in prod
    TEMP_THRESH = 1
    for fn in filenames:
        # TODO clean this part in prod
        if processed >= TEMP_THRESH:
            break
        tweets = get_tweets(os.path.join(input_dir, fn))
        tweets_cleaned = TwProcessor.remove_redundant_tweets(tweets, twid_fn)
        tweets_cleaned = TwProcessor.remove_non_en_tweets(tweets_cleaned)
        tweets_cleaned = TwProcessor.remove_unnecessary_fields(tweets_cleaned)
        tweets_cleaned = TwProcessor.tokenize(tweets_cleaned, exclude_kwlist)
        tweets_cleaned = TwProcessor.gen_kwpairs(tweets_cleaned)
        # replace the .txt suffix by .json to make the filetype
        # self-corresponding
        fn = fn[0:-3] + "json"
        out_fn = os.path.join(output_dir, fn)
        jsondump(tweets_cleaned, out_fn)
        processed += 1
        if processed % 10 == 0:
            INFO('{}/{} processed'.format(processed, total))


class TwProcessor:
    ''' This is the class containing only static methods '''
    @staticmethod
    def remove_redundant_tweets(tweets, twid_fn):
        ''' Remove the redundant tweets as per the existing tweet IDs '''
        tweets_cleaned = []
        tids = jsonload(twid_fn)
        if tids is None:
            tids = []
        # convert to set to optimize the searching
        tids = set(tids)
        for t in tweets:
            while 'retweeted_status' in t:
                t = t['retweeted_status']
            tid = t['id']
            if tid in tids or tid < 100000:
                continue
            # tid not in tidlist meaning it is unseen thus nonredundant
            tids.add(tid)
            tweets_cleaned.append(t)
        # convert back to list for json dump
        tids = list(tids)
        jsondump(tids, twid_fn)
        return tweets_cleaned

    @staticmethod
    def remove_non_en_tweets(tweets):
        ''' Remove the non EN tweets '''
        tweets_cleaned = []
        for t in tweets:
            if t['lang'] != 'en':
                continue
            if 'retweeted_status' in t:
                INFO("Should not happen at all")
                if t['retweeted_status']['lang'] != 'en':
                    continue
            tweets_cleaned.append(t)
        return tweets_cleaned

    @staticmethod
    def remove_unnecessary_fields(tweets):
        ''' Remove the unnecessary fields in the tweets to save some storage '''
        cfg = get_config()
        twkeys = cfg['storyline']['preprocess']['twkeys']
        userkeys = cfg['storyline']['preprocess']['userkeys']
        tweets_cleaned = []
        for tweet in tweets:
            tc = {}
            for k in twkeys:
                if k not in tweet:
                    continue
                tc[k] = tweet[k]
            # we need to simplify the user fields
            tu = {}
            for k in userkeys:
                if k not in tweet['user']:
                    continue
                tu[k] = tweet['user'][k]
            tc['user'] = tu
            tweets_cleaned.append(tc)
        return tweets_cleaned

    @staticmethod
    def tokenize(tweets, search_words):
        ''' Tokenize the tweet text '''
        cfg = get_config()
        len_thresh = cfg['storyline']['preprocess']['kw_len_thresh']
        stopwords = cfg['storyline']['preprocess']['stopwords']
        return [t for t in tok(tweets, len_thresh, stopwords, search_words)
                if len(t['keywords']) >= 2]

    @staticmethod
    def gen_kwpairs(tweets):
        ''' generate the kw pairs from the tweets' keywords '''
        for t in tweets:
            t['kwpairs'] = [i for i in itertools.product(t['keywords'],
                                                         t['keywords'])
                            if i[0] < i[1]]
        return tweets


if __name__ == '__main__':
    cfg = get_config()
    input_dir = os.path.join(cfg['data']['srcdir'],
                             cfg['data']['protests']['subdir'])
    output_dir = os.path.join(cfg['data']['outdir'],
                              cfg['storyline']['datadir'],
                              cfg['storyline']['preprocess']['datadir'])
    print input_dir
    print output_dir
    preprocess(input_dir, output_dir, cfg['data']['protests']['searchwords'])
