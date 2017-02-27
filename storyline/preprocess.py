#!/usr/bin/env python

import os

from apollo_lib.apolloutil import read_and_parse_tweets_from_file as get_tweets
from trackutil.ioutil import jsonload, jsondump
from trackutil.pathutil import mkdir, get_datafiles_in_dir
from trackutil.pathutil import get_storyline_module_dir
from trackutil.confutil import get_config
from trackutil.logger import INFO
from trackutil.textutil import tokenize as tok


def preprocess_wrapper(input_dir, output_dir, exclude_kwlist):
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
    # the threshold of the number of files to be considered, -1 mean unlimited
    batchsize = cfg['storyline']['preprocess']['batchsize']
    for fn in filenames:
        if batchsize >= 0 and processed >= batchsize:
            break
        inputfn = os.path.join(input_dir, fn)
        fn = fn[0:-3] + "json"
        outputfn = os.path.join(output_dir, fn)
        preprocess(inputfn, outputfn, exclude_kwlist, twid_fn)
        processed += 1
        if processed % 10 == 0:
            INFO('{}/{} processed'.format(processed, total))


def preprocess(ts, cfg=None):
    '''
    Preprocess to remove redundant tweets, also exclude the keyword in keyword
    '''
    INFO('[Preprocess] {}'.format(ts))
    if cfg is None:
        cfg = get_config()

    input_dir = cfg['data']['srcdir']
    output_dir = get_storyline_module_dir(cfg, 'preprocess')
    mkdir(output_dir)

    input_fn_suffix = cfg['data']['fn_suffix']
    inputfn = '{}{}'.format(ts, input_fn_suffix)
    inputfn = os.path.join(input_dir, inputfn)

    outputfn = '{}.json'.format(ts)
    outputfn = os.path.join(output_dir, outputfn)

    searchwords = cfg['data']['searchwords']

    tweets = get_tweets(inputfn)
    tweets_cleaned = TwProcessor.remove_redundant_tweets(tweets)
    tweets_cleaned = TwProcessor.remove_unnecessary_fields(tweets_cleaned, cfg)
    tweets_cleaned = TwProcessor.remove_nonquery_tweets(
        tweets_cleaned, searchwords)
    tweets_cleaned = TwProcessor.tokenize(tweets_cleaned, searchwords)
    tweets_cleaned = TwProcessor.gen_kwpairs(tweets_cleaned)
    jsondump(tweets_cleaned, outputfn)


class TwProcessor:
    ''' This is the class containing only static methods '''
    @staticmethod
    def remove_redundant_tweets(tweets, twid_fn=None, size=500000):
        ''' Remove the redundant tweets as per the existing tweet IDs '''
        tweets_cleaned = []
        tids = []
        if twid_fn is not None:
            tids = jsonload(twid_fn)
            if tids is None:
                tids = []
        # convert to set to optimize the searching
        tidset = set(tids)
        for t in tweets:
            while 'retweeted_status' in t:
                t = t['retweeted_status']
            tid = t['id']
            if tid in tidset or tid < 100000:
                continue
            # tid not in tidlist meaning it is unseen thus nonredundant
            tidset.add(tid)
            tids.append(tid)
            tweets_cleaned.append(t)
        # Note that we want to keep the meta data file with handlable size
        # thus only the last 500000 tids are kept
        if twid_fn is not None:
            jsondump(tids[-size:], twid_fn)
        return tweets_cleaned

    @staticmethod
    def remove_nonquery_tweets(tweets, searchwords):
        tweets_cleaned = []
        for t in tweets:
            for w in searchwords:
                if w in t['text'].lower():
                    tweets_cleaned.append(t)
                    # since the searchwords are OR'ed
                    break
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
    def remove_unnecessary_fields(tweets, cfg=None):
        ''' Remove the unnecessary fields in the tweets to save some storage '''
        if cfg is None:
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
            if 'user' in twkeys:
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
        tweets_cleaned = tok(tweets, len_thresh, stopwords, search_words)
        for t in tweets_cleaned:
            t['keywords'] = sorted(list(set(t['keywords'])))
        return [t for t in tweets_cleaned if len(t['keywords']) >= 3]

    @staticmethod
    def gen_kwpairs(tweets):
        '''
        generate the kw pairs from the tweets' keywords
        Note that here we apply one heuristic:
            We do NOT consider adjacent keywords as kw pair, because sometimes
            the name of a person or a place (like Donald Trump, New York)
            contains two adjacent keywords. Ignoring the adjacent keywords can
            avoid considering the *highly correlated* token pairs as keyword
            pairs.
        '''
        for t in tweets:
            t['kwpairs'] = []
            for i in range(len(t['keywords']) - 2):
                for j in range(i + 2, len(t['keywords'])):
                    pair = tuple(sorted([t['keywords'][i], t['keywords'][j]]))
                    t['kwpairs'].append(pair)
        return tweets


def main():
    cfg = get_config()
    input_dir = os.path.join(cfg['data']['srcdir'],
                             cfg['data']['protests']['subdir'])
    output_dir = os.path.join(cfg['data']['outdir'],
                              cfg['storyline']['datadir'],
                              cfg['storyline']['preprocess']['datadir'])
    print input_dir
    print output_dir
    preprocess_wrapper(
        input_dir, output_dir, cfg['data']['protests']['searchwords'])


if __name__ == "__main__":
    main()
