import os
import statistics

from networkx import Graph, is_connected, connected_component_subgraphs
from operator import itemgetter

from trackutil.alg import jaccard_similarity_var
from trackutil.confutil import get_config
from trackutil.logger import INFO
from trackutil.ioutil import jsonload, jsondump
from trackutil.pathutil import get_datafiles_in_dir, mkdir
from trackutil.pathutil import get_storyline_module_dir
from trackutil.textutil import tokenize_text


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


def tokenize_text_wrapper(text, cfg):
    kw_exclude_set = cfg['storyline']['preprocess']['stopwords']
    len_thresh = 0  # basically there is no length threshold
    searchwords = cfg['data']['searchwords']
    tokens = list(set(tokenize_text(
        text, len_thresh, kw_exclude_set, searchwords)))
    return tokens


def build_cluster_graph(tweets, cfg):
    cmp_list = [(i, j) for i in range(len(tweets)) for j in range(len(tweets))
                if i < j]
    INFO('Got cmp_list')
    G = Graph()
    processed = 0
    total = len(cmp_list)
    for item in cmp_list:
        i, j = item
        t1 = tweets[i]
        t2 = tweets[j]
        tokens1 = tokenize_text_wrapper(t1[1], cfg)
        tokens2 = tokenize_text_wrapper(t2[1], cfg)
        id1 = t1[0]
        id2 = t2[0]
        similarity = jaccard_similarity_var(tokens1, tokens2)
        if similarity > 0:
            G.add_edge(id1, id2, weight=similarity)
        processed += 1
        if processed % 1000 == 0:
            INFO('Processed {}/{}'.format(processed, total))
    return G


def split_graph(G, id_tweet_map, partition_thresh, cfg):
    '''
    Split a complete graph G into two sub-graphs.
    Returns the connected components
    '''
    tweets = [id_tweet_map[i] for i in G.nodes()]
    if partition_terminate(tweets, cfg):
        return [G]
    ret = []
    edges = G.edges(data=True)
    sorted_edges = sorted(
        edges, key=lambda (source, target, data): data['weight'])
    processed = 0
    for i in range(len(sorted_edges)):
        if not is_connected(G):
            break
        processed += 1
        if processed % 100 == 0:
            INFO('Edge {}, {}, {}'.format(*sorted_edges[i]))
        src, dst, weight = sorted_edges[i]
        # We do not want to over partition the graph
        if weight > partition_thresh:
            break
        G.remove_edge(src, dst)
    sub_graphs_generator = connected_component_subgraphs(G)
    sub_graphs = []
    for sg in sub_graphs_generator:
        sub_graphs.append(sg)
    if len(sub_graphs) == 1:
        # if only one component, it means that we stopped to avoid over
        # partitioning, therefore, no need to further partition, we just
        # return this component
        return [sub_graphs[0]]
    # If we end up with multiple component, then it must be exactly two
    # components.
    assert len(sub_graphs) == 2
    sg1, sg2 = sub_graphs
    INFO("Split")
    ret.extend(split_graph(sg1, id_tweet_map, partition_thresh, cfg))
    ret.extend(split_graph(sg2, id_tweet_map, partition_thresh, cfg))
    return ret


def partition_tweets(tweets, partition_thresh, cfg):
    G = build_cluster_graph(tweets, cfg)
    INFO('Graph built')
    tid_tweet_map = {t[0]: t for t in tweets}
    components = split_graph(G, tid_tweet_map, partition_thresh, cfg)
    tweets_partition = []
    for component in components:
        sub_tweets = [tid_tweet_map[i] for i in component.nodes()]
        tweets_partition.append(sub_tweets)
    return tweets_partition


def partition_terminate(tweets, cfg):
    len_thresh = 5
    if len(tweets) < len_thresh:
        return True
    return is_event_tweet_cluster(tweets, cfg)


def is_event_tweet_cluster(tweets, cfg):
    cfg = get_config()
    mean_thresh = cfg['storyline']['purify']['beta_mean_thresh']
    token_distribution = gen_token_distribution_tweets(tweets, cfg)
    m = statistics.mean(token_distribution)
    return m > mean_thresh


def is_event_bucket(bucket, cfg):
    return is_event_tweet_cluster(bucket[1], cfg)


def partition_bucket(bucket, partition_thresh, cfg):
    signature = bucket[0]
    tweets = bucket[1]
    INFO('The number of tweets is {}'.format(len(tweets)))
    tweets_partition = partition_tweets(tweets, partition_thresh, cfg)
    ret = []
    for i in range(len(tweets_partition)):
        if len(tweets_partition[i]) < 5:
            continue
        #  if not is_event_tweet_cluster(tweets_partition[i], cfg):
        #      continue
        si = '{0}___{1}'.format(signature, i)
        sub_bucket = []
        sub_bucket.append(si)
        sub_bucket.append(tweets_partition[i])
        ret.append(sub_bucket)
    return ret


def partition_buckets(buckets, partition_thresh, cfg):
    ret = []
    processed = 0
    for b in buckets:
        ret.extend(partition_bucket(b, partition_thresh, cfg))
        processed += 1
        INFO('Processed {} buckets'.format(processed))
        if processed == 7:
            break
    return ret


def partition_huge_buckets_wrapper(inputfn,
                                   outputfn,
                                   partition_thresh,
                                   huge_cluster_thresh,
                                   cfg):
    buckets = jsonload(inputfn)
    if buckets is None:
        return
    buckets = buckets.items()
    normal_buckets = [b for b in buckets if len(b[1]) <= huge_cluster_thresh]
    # We discard the large cluster since they usually contain garbage
    # and processing them would be too slow
    huge_buckets = [b for b in buckets if len(b[1]) > huge_cluster_thresh and
                    len(b[1]) <= 200]
    INFO('Num of normal_buckets is {} and num of huge_buckets is {}'.format(
        len(normal_buckets), len(huge_buckets)))
    INFO('normal size: {}, huge size: {}'.format(len(normal_buckets),
                                                 len(huge_buckets)))
    partitioned_buckets = partition_buckets(huge_buckets, partition_thresh, cfg)
    ret = normal_buckets
    ret.extend(partitioned_buckets)
    jsondump(ret, outputfn)


def main():
    cfg = get_config()
    root_dir = cfg['data']['outdir']
    root_dir = os.path.join(root_dir, cfg['storyline']['datadir'])
    input_dir = os.path.join(root_dir,
                             cfg['storyline']['absorb']['datadir'])
    output_dir = os.path.join(root_dir,
                              cfg['storyline']['partition']['datadir'])
    mkdir(output_dir)
    partition_thresh = cfg['storyline']['partition']['partition_thresh']
    huge_cluster_thresh = cfg['storyline']['partition']['huge_cluster_thresh']
    fnlist = get_datafiles_in_dir(input_dir)
    #  fnlist = ['{}.json'.format(fn) for fn in range(610, 626)]
    processed = 0
    total = len(fnlist)
    for fn in fnlist:
        inputfn = os.path.join(input_dir, fn)
        outputfn = os.path.join(output_dir, fn)
        partition_huge_buckets_wrapper(inputfn, outputfn,
                                       partition_thresh,
                                       huge_cluster_thresh,
                                       cfg)
        processed += 1
        if processed % 10 == 0:
            INFO('processed {}/{}'.format(processed, total))


def partition_new(ts, cfg=None):
    INFO('[Partition] {}'.format(ts))
    if cfg is None:
        cfg = get_config()
    inputdir = get_storyline_module_dir(cfg, 'absorb')
    outputdir = get_storyline_module_dir(cfg, 'partition')
    mkdir(outputdir)
    partition_thresh = cfg['storyline']['partition']['datadir']
    huge_cluster_thresh = cfg['storyline']['partition']['huge_cluster_thresh']
    inputfn = os.path.join(inputdir, '{}.json'.format(ts))
    outputfn = os.path.join(outputdir, '{}.json'.format(ts))
    partition_huge_buckets_wrapper(inputfn, outputfn, partition_thresh,
                                   huge_cluster_thresh, cfg)


if __name__ == '__main__':
    main()
