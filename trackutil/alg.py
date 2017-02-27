#!/usr/bin/env python

from collections import Counter

import math
import statistics


def cal_mean(l):
    '''
    calculate the mean of a list of values
    '''
    return statistics.mean(l)


def cal_var(l):
    '''
    calculate the variance of a list of values
    '''
    return statistics.variance(l)


def cal_mean_var(l):
    '''
    return the mean and variance at the same time
    '''
    return (cal_mean(l), cal_var(l))


def jaccard(b1, b2, top_k=-1):
    '''
    Calculate the Jaccard distance tween two buckets
    '''
    tokenlist_list1 = [t[3] for t in b1['tweets']]
    tokenlist_list2 = [t[3] for t in b2['tweets']]
    token_counter1 = get_token_counter(tokenlist_list1)
    token_counter2 = get_token_counter(tokenlist_list2)
    if top_k > 0:
        token_dict1 = dict(token_counter1.most_common(top_k))
        token_dict2 = dict(token_counter2.most_common(top_k))
    else:
        token_dict1 = dict(token_counter1)
        token_dict2 = dict(token_counter2)
    dist = jaccard_dist(token_dict1.keys(), token_dict2.keys())
    return dist


def get_token_counter(tokenlist_list):
    '''
    Get the token dict from a list of token lists
    '''
    c = [Counter(l) for l in tokenlist_list]
    return reduce(lambda x, y: x + y, c)


def jaccard_dist(s1, s2):
    '''
    Jaccard dist = 1 - Jaccard similarity
    '''
    return 1 - jaccard_similarity(s1, s2)


def jaccard_similarity(s1, s2):
    '''
    Jaccard similarity, the inputs will be two token collections
    '''
    s1 = set(s1)
    s2 = set(s2)
    setu = s1.union(s2)
    seti = s1.intersection(s2)
    return jaccard_similarity_core(seti, setu)


def jaccard_similarity_var(s1, s2):
    '''
    A variant of Jaccard similarity, the inputs are the same as the normal one
    '''
    s1 = set(s1)
    s2 = set(s2)
    seti = s1.intersection(s2)
    if len(s1) < len(s2):
        setu = s1
    else:
        setu = s2
    return jaccard_similarity_core(seti, setu)


def jaccard_similarity_core(seti, setu):
    if len(setu) == 0:
        return 1.0
    return len(seti) * 1.0 / len(setu)


def ig(a, b, c, d):
    '''
    Calculate the Information Gain.
    Input:
        a: the number of tweets in the pre window and containing the kwp
        b: ........................... cur .............................
        c: the number of tweets in the pre window and not containing the kwp
        d: ........................... cur .................................

        Graphically,
                             Y = pre | Y = cur
            X = presence |      a    |    b
            X = absence  |      c    |    d

        H(Y) = -((a+c)/(a+b+c+d))log((a+c)/(a+b+c+d))
               -((b+d)/(a+b+c+d))log((b+d)/(a+b+c+d))

        H(X) = -((a+b)/(a+b+c+d))log((a+b)/(a+b+c+d))
               -((c+d)/(a+b+c+d))log((c+d)/(a+b+c+d))

        H(X,Y) = -(a/(a+b+c+d))log(a/(a+b+c+d))
                 -(b/(a+b+c+d))log(b/(a+b+c+d))
                 -(c/(a+b+c+d))log(c/(a+b+c+d))
                 -(d/(a+b+c+d))log(d/(a+b+c+d))

        IG(X,Y) = H(X) + H(Y) - H(X,Y)

    Output:
        IG
    '''
    hx = entropy((a + b, c + d))
    hy = entropy((a + c, b + d))
    hxy = entropy((a, b, c, d))
    return hx + hy - hxy


def entropy(v):
    '''
    Calculate the entropy between a list of integers

        H(v) = - \sum_i xlogx(i/sum(v))
    '''
    # just ignore the 0 case by adding a very small positive delta
    v = [i + 1e-9 for i in v]
    for i in v:
        assert(i > 0.0)
    num = sum(v) * 1.0
    return -sum([xlogx(i * 1.0 / num) for i in v])


def xlogx(x):
    ''' Calculate x*log_2(x), based 2 '''
    return x * 1.0 * math.log(x * 1.0, 2)
