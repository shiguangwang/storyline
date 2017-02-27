#!/usr/bin/env python

import re
import string

from nltk.corpus import stopwords

from logger import DEBUG

from trackutil.alg import jaccard_similarity_var


def normalize(text):
    '''
    We normalize the text and return the space separated text
    '''
    text = text.lower()
    text = re.sub(r'http:(.*)|https:(.*)', '', text)
    # Ignore the @xxx tokens
    text = re.sub(r'@([a-za-z0-9_])+', '', text)
    # replace the special chars by space
    for ch in u'''!~"#$%&()*+,-./:;<=>?@[\\]?_'`{|}?\n\u2018\u2019''':
        text = string.replace(text, ch, ' ')
    return text.encode('ascii', 'ignore')


def tokenize(tweets, len_thresh, kw_exclude_set, search_words):
    '''
    Extract the keywords from tweet text and extend the tweet dic with a new
    key field 'keywords'
    '''
    DEBUG('Start getting the keyword tokens ...')
    processed = 0
    total = len(tweets)
    for tweet in tweets:
        processed = processed + 1
        if int(processed * 100.0 / total) % 5 == 0:
            DEBUG('progress: {0}/{1}'.format(processed, total))
        cleaned = tokenize_text(tweet['text'], len_thresh, kw_exclude_set,
                                search_words)
        tweet['keywords'] = cleaned
    return tweets


def tokenize_text(text, len_thresh, kw_exclude_set, search_words):
    '''
    Tokenize the tweet text
    '''
    stop = stopwords.words('english')
    text = normalize(text)
    tokens = [i for i in text.split(' ')
              if i not in kw_exclude_set and len(i) >= len_thresh and
              i not in stop]
    cleaned = [t for t in tokens
               if len([i for i in search_words if t in i]) == 0]
    return cleaned


def tokenize_text_wrapper(text, cfg):
    len_thresh = 1
    kw_exclude_set = set([])
    search_words = cfg['data']['searchwords']
    return tokenize_text(text, len_thresh, kw_exclude_set, search_words)


def bipartite_merge(bucket1, bucket2, tweet_similarity_thresh,
                    bucket_merge_thresh, cfg):
    '''
    The idea is that for a smaller bucket there maybe a large portion of the
    tweets contained in another bucket, thus we need to merge the two buckets.
    '''
    similarity_thresh = tweet_similarity_thresh
    absorb_thresh = bucket_merge_thresh
    if len(bucket1) < len(bucket2):
        small_bucket, large_bucket = bucket1, bucket2
    else:
        small_bucket, large_bucket = bucket2, bucket1
    # the number of tweets in the small bucket contained in the larger one.
    belonging_cnt = 0
    large_tids = set([t[0] for t in large_bucket])
    for t in small_bucket:
        if t[0] in large_tids:
            belonging_cnt += 1
            continue
        # in this case, the text could be similar as well
        t_tokens = tokenize_text_wrapper(t[1], cfg)
        for r in large_bucket:
            r_tokens = tokenize_text_wrapper(r[1], cfg)
            similarity = jaccard_similarity_var(t_tokens, r_tokens)
            if similarity >= similarity_thresh:
                belonging_cnt += 1
                #  INFO('Found a dup text: {}\n\t{}'.format(t[1], r[1]))
                break
    if belonging_cnt * 1.0 / len(small_bucket) >= absorb_thresh:
        return 0.0  # very similar
    return 1.0  # very different
