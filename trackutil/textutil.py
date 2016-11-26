#!/usr/bin/env python

import re
import string

from nltk.corpus import stopwords

from logger import DEBUG


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
    stop = stopwords.words('english')
    processed = 0
    total = len(tweets)
    for tweet in tweets:
        processed = processed + 1
        if int(processed * 100.0 / total) % 5 == 0:
            DEBUG('progress: {0}/{1}'.format(processed, total))
        tweet_text = normalize(tweet['text'])
        tokens = [i for i in tweet_text.split(' ')
                  if i not in kw_exclude_set and len(i) >= len_thresh and
                  i not in stop]
        cleaned = [t for t in tokens
                   if len([i for i in search_words if t in i]) == 0]
        tweet['keywords'] = cleaned
    return tweets
