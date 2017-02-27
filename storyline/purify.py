import os
import statistics

from operator import itemgetter

from trackutil.confutil import get_config
from trackutil.ioutil import jsonload, jsondump
from trackutil.textutil import tokenize_text
from trackutil.logger import INFO
from trackutil.pathutil import mkdir, get_datafiles_in_dir
from trackutil.pathutil import get_storyline_module_dir


def tokenize_text_wrapper(text, cfg):
    kw_exclude_set = cfg['storyline']['preprocess']['stopwords']
    len_thresh = 1  # basically there is no length threshold
    searchwords = cfg['data']['searchwords']
    tokens = list(set(tokenize_text(
        text, len_thresh, kw_exclude_set, searchwords)))
    return tokens


def gen_dist_tweets(tweets, cfg):
    token_dist = {}
    for tweet in tweets:
        text = tweet[1]
        tokens = tokenize_text_wrapper(text, cfg)
        for token in tokens:
            if token not in token_dist:
                token_dist[token] = 0
            token_dist[token] += 1
    token_dist_items = token_dist.items()
    token_dist_items = sorted(token_dist_items, key=itemgetter(1), reverse=True)
    return token_dist_items


def gen_token_distribution_tweets(tweets, cfg):
    token_dist_items = gen_dist_tweets(tweets, cfg)
    cnt = len(tweets) * 1.0
    return [i[1] * 1.0 / cnt for i in token_dist_items]


def is_event_tweet_cluster(tweets, mean_thresh, cfg):
    token_distribution = gen_token_distribution_tweets(tweets, cfg)
    m = statistics.mean(token_distribution)
    return m > mean_thresh


def is_event_bucket(bucket, mean_thresh, cfg):
    return is_event_tweet_cluster(bucket[1], mean_thresh, cfg)


def purify(buckets, mean_thresh, cfg):
    ret = [b for b in buckets if is_event_bucket(b, mean_thresh, cfg)]
    return ret


def purify_wrapper(inputfn, outputfn, cfg):
    mean_thresh = cfg['storyline']['purify']['beta_mean_thresh']
    buckets = jsonload(inputfn)
    if buckets is None:
        return
    #  buckets = buckets.items()
    size_thresh = cfg['storyline']['purify']['bucket_size_thresh']
    buckets = [b for b in buckets if len(b[1]) < size_thresh]
    purified = purify(buckets, mean_thresh, cfg)
    jsondump(purified, outputfn)


def main():
    cfg = get_config()
    rootdir = cfg['data']['outdir']
    rootdir = os.path.join(rootdir, cfg['storyline']['datadir'])
    inputdir = os.path.join(rootdir, cfg['storyline']['partition']['datadir'])
    outputdir = os.path.join(rootdir, cfg['storyline']['purify']['datadir'])
    mkdir(outputdir)
    fnlist = get_datafiles_in_dir(inputdir)
    #  fnlist = ['{}.json'.format(fn) for fn in range(610, 626)]
    processed = 0
    total = len(fnlist)
    for fn in fnlist:
        inputfn = os.path.join(inputdir, fn)
        outputfn = os.path.join(outputdir, fn)
        purify_wrapper(inputfn, outputfn, cfg)
        processed += 1
        INFO('Processed {}/{}'.format(processed, total))


def purify_new(ts, cfg=None):
    INFO('[Purify] {}'.format(ts))
    if cfg is None:
        cfg = get_config()
    inputdir = get_storyline_module_dir(cfg, 'partition')
    outputdir = get_storyline_module_dir(cfg, 'purify')
    mkdir(outputdir)
    inputfn = os.path.join(inputdir, '{}.json'.format(ts))
    outputfn = os.path.join(outputdir, '{}.json'.format(ts))
    purify_wrapper(inputfn, outputfn, cfg)


if __name__ == '__main__':
    main()
