#!/usr/bin/env python

import csv
import statistics

from networkx import Graph, is_connected, connected_component_subgraphs
from operator import itemgetter
from scipy.stats import beta

from trackutil.alg import jaccard_similarity_var
from trackutil.confutil import get_config
from trackutil.textutil import tokenize_text
from trackutil.ioutil import jsondump, jsonload
from trackutil.logger import INFO


def gen_dist(buckets):
    ret = []
    for bucket in buckets:
        bucket = gen_dist_bucket(bucket)
        ret.append(bucket)
    return ret


def tokenize_text_wrapper(text):
    cfg = get_config()
    kw_exclude_set = cfg['storyline']['preprocess']['stopwords']
    len_thresh = 1  # basically there is no length threshold
    searchwords = cfg['data']['protests']['searchwords']
    tokens = list(set(tokenize_text(text, len_thresh, kw_exclude_set, searchwords)))
    return tokens


def gen_dist_bucket(bucket):
    tweets = bucket[1]
    token_dist_items = gen_dist_tweets(tweets)
    bucket.append(token_dist_items)
    return bucket


def gen_dist_tweets(tweets):
    token_dist = {}
    for tweet in tweets:
        text = tweet[1]
        tokens = tokenize_text_wrapper(text)
        for token in tokens:
            if token not in token_dist:
                token_dist[token] = 0
            token_dist[token] += 1
    token_dist_items = token_dist.items()
    token_dist_items = sorted(token_dist_items, key=itemgetter(1), reverse=True)
    return token_dist_items


def gen_token_distribution_tweets(tweets):
    token_dist_items = gen_dist_tweets(tweets)
    cnt = len(tweets) * 1.0
    return [i[1] * 1.0 / cnt for i in token_dist_items]


def gen_dist_wrapper(inputfn, outputfn):
    buckets = jsonload(inputfn)
    buckets_with_dist = gen_dist(buckets)
    jsondump(buckets_with_dist, outputfn)


def summarize_dist(buckets_with_dist):
    buckets = buckets_with_dist
    ret = []
    max_len = -1
    for bucket in buckets:
        dist_items = bucket[3]
        summary = [i[1] for i in dist_items]
        ret.append(summary)
        if len(summary) > max_len:
            max_len = len(summary)
    for summary in ret:
        if len(summary) < max_len:
            for i in range(max_len - len(summary)):
                summary.append(0)
    return ret


def summarize_portion_dist(buckets):
    ret = []
    max_len = -1
    for bucket in buckets:
        dist_items = bucket[3]
        tweet_cnt = len(bucket[1])
        summary = [i[1] * 1.0 / tweet_cnt for i in dist_items]
        ret.append(summary)
        if len(summary) > max_len:
            max_len = len(summary)
    for summary in ret:
        if len(summary) < max_len:
            for i in range(max_len - len(summary)):
                summary.append(0.0)
    return ret


def summarize_portion_dist_wrapper(inputfn, outputfn):
    buckets = jsonload(inputfn)
    buckets_with_portion_dist = summarize_portion_dist(buckets)
    with open(outputfn, 'wb') as f:
        writer = csv.writer(f)
        writer.writerows(buckets_with_portion_dist)


def summarize_dist_wrapper(inputfn, outputfn):
    buckets = jsonload(inputfn)
    summaries = summarize_dist(buckets)
    with open(outputfn, 'wb') as f:
        writer = csv.writer(f)
        writer.writerows(summaries)


def beta_fit(buckets):
    ret = []
    for bucket in buckets:
        dist_items = bucket[3]
        tweet_cnt = len(bucket[1])
        summary = [i[1] * 1.0 / tweet_cnt for i in dist_items]
        p1, p2, _, _ = beta.fit(summary)
        bucket.append((p1, p2))
        ret.append(bucket)
    return ret


def beta_fit_wrapper(inputfn, outputfn):
    buckets = jsonload(inputfn)
    buckets = beta_fit(buckets)
    jsondump(buckets, outputfn)


def beta_fit_csv(buckets):
    ret = []
    for bucket in buckets:
        ret.append(bucket[4])
    return ret


def beta_fit_csv_wrapper(inputfn, outputfn):
    buckets = jsonload(inputfn)
    params = beta_fit_csv(buckets)
    with open(outputfn, 'wb') as f:
        writer = csv.writer(f)
        writer.writerows(params)


def expectation_separator(buckets):
    pos = []
    neg = []
    expectation_thresh = 0.03
    for bucket in buckets:
        alpha, beta = bucket[4]
        expectation = alpha / (alpha + beta)
        if expectation > expectation_thresh:
            pos.append(bucket)
        else:
            neg.append(bucket)
    return pos, neg


def expectation_separator_wrapper(inputfn, posfn, negfn):
    buckets = jsonload(inputfn)
    pos, neg = expectation_separator(buckets)
    jsondump(pos, posfn)
    jsondump(neg, negfn)


def text_summary(inputfn, outputfn):
    buckets = jsonload(inputfn)
    text_summary_buckets(buckets, outputfn)


def text_summary_buckets(buckets, outputfn):
    processed = 0
    with open(outputfn, 'w') as f:
        for b in buckets:
            processed += 1
            f.write('==================================\n')
            f.write('{0}. {1}\n'.format(processed, b[0]))
            f.write('\n')
            for t in b[1]:
                f.write('\t*** {0}\n'.format(t[1].encode('utf-8', 'ignore')))
            f.write('\n')


def whole_beta_fit(buckets):
    seq = []
    for b in buckets:
        cnt = len(b[1])
        freq = [i[1] * 1.0 / cnt for i in b[3]]
        seq.extend(freq)
    alpha, bb, _, _ = beta.fit(seq)
    return alpha, bb


def whole_beta_fit_wrapper(inputfn):
    buckets = jsonload(inputfn)
    alpha, bb = whole_beta_fit(buckets)
    print alpha, bb


def labeled_huge_cluster_parser(inputfn, outputfn):
    buckets = []
    with open(inputfn) as f:
        lines = f.readlines()
    bucket = []
    tweets = []
    for i in range(len(lines)):
        line = lines[i].strip()
        if line.startswith('===='):
            # This is the bucket separator
            if len(bucket) > 0:
                buckets.append(bucket)
            bucket = []
            i = i + 1
            line = lines[i].strip()
            bucket.append(line.split('. ')[1])
            bucket.append([])
            i = i + 1
            line = lines[i]
        elif line.startswith('***'):
            print line
            tweets.append(line.split('*** ')[1])
        elif line.startswith('------'):
            bucket[1].append(tweets)
            tweets = []
    assert len(tweets) == 0
    if len(bucket) > 0:
        buckets.append(bucket)
    jsondump(buckets, outputfn)


def cal_intra_cluster_similarity(cluster):
    ret = []
    tokens_list = [tokenize_text_wrapper(t) for t in cluster]
    cmp_list = [(i, j) for i in range(len(tokens_list)) for j in range(len(tokens_list)) if i < j]
    for pair in cmp_list:
        i, j = pair
        ret.append(jaccard_similarity_var(tokens_list[i], tokens_list[j]))
    return ret


def cal_inter_cluster_similarity(c1, c2):
    ret = []
    t1_list = [tokenize_text_wrapper(t) for t in c1]
    t2_list = [tokenize_text_wrapper(t) for t in c2]
    cmp_list = [(i, j) for i in range(len(t1_list)) for j in range(len(t2_list))]
    for pair in cmp_list:
        i, j = pair
        ret.append(jaccard_similarity_var(t1_list[i], t2_list[j]))
    return ret


def huge_cluster_similarity_stat(buckets):
    inter_cluster_similarity = []
    intra_cluster_similarity = []
    for bucket in buckets:
        clusters = bucket[1]
        for cluster in clusters:
            intra_cluster_similarity.extend(cal_intra_cluster_similarity(cluster))
        cluster_comp = [(clusters[i], clusters[j]) for i in range(len(clusters)) for j in range(len(clusters)) if i < j]
        for item in cluster_comp:
            inter_cluster_similarity.extend(cal_inter_cluster_similarity(*item))
    return inter_cluster_similarity, intra_cluster_similarity


def huge_cluster_similarity_stat_wrapper(inputfn, interfn, intrafn):
    buckets = jsonload(inputfn)
    inter, intra = huge_cluster_similarity_stat(buckets)
    with open(interfn, 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(inter)
    with open(intrafn, 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(intra)


def build_cluster_graph(tweets):
    cmp_list = [(i, j) for i in range(len(tweets)) for j in range(len(tweets)) if i < j]
    INFO('Got cmp_list')
    G = Graph()
    processed = 0
    total = len(cmp_list)
    for item in cmp_list:
        i, j = item
        t1 = tweets[i]
        t2 = tweets[j]
        tokens1 = tokenize_text_wrapper(t1[1])
        tokens2 = tokenize_text_wrapper(t2[1])
        id1 = t1[0]
        id2 = t2[0]
        similarity = jaccard_similarity_var(tokens1, tokens2)
        if similarity > 0:
            G.add_edge(id1, id2, weight=similarity)
        processed += 1
        if processed % 1000 == 0:
            INFO('Processed {}/{}'.format(processed, total))
    return G


def split_graph(G, id_tweet_map, partition_thresh=0.15):
    '''
    Split a complete graph G into two sub-graphs.
    Returns the connected components
    '''
    tweets = [id_tweet_map[i] for i in G.nodes()]
    if partition_terminate(tweets):
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
    ret.extend(split_graph(sg1, id_tweet_map))
    ret.extend(split_graph(sg2, id_tweet_map))
    return ret


def partition_tweets(tweets, partition_thresh=0.15):
    G = build_cluster_graph(tweets)
    INFO('Graph built')
    tid_tweet_map = {t[0]: t for t in tweets}
    components = split_graph(G, tid_tweet_map, partition_thresh)
    tweets_partition = []
    for component in components:
        sub_tweets = [tid_tweet_map[i] for i in component.nodes()]
        tweets_partition.append(sub_tweets)
    return tweets_partition


def partition_terminate(tweets):
    len_thresh = 5
    if len(tweets) < len_thresh:
        return True
    return is_event_tweet_cluster(tweets)


def is_event_tweet_cluster(tweets):
    mean_thresh = 0.3
    token_distribution = gen_token_distribution_tweets(tweets)
    m = statistics.mean(token_distribution)
    return m > mean_thresh


def is_event_bucket(bucket):
    return is_event_tweet_cluster(bucket[1])


def partition_bucket(bucket, partition_thresh=0.15):
    signature = bucket[0]
    tweets = bucket[1]
    INFO('The number of tweets is {}'.format(len(tweets)))
    tweets_partition = partition_tweets(tweets, partition_thresh)
    ret = []
    for i in range(len(tweets_partition)):
        if len(tweets_partition[i]) < 5:
            continue
        if not is_event_tweet_cluster(tweets_partition[i]):
            continue
        si = '{0}___{1}'.format(signature, i)
        sub_bucket = []
        sub_bucket.append(si)
        sub_bucket.append(tweets_partition[i])
        ret.append(sub_bucket)
    return ret


def partition_buckets(buckets, partition_thresh=0.15):
    ret = []
    processed = 0
    for b in buckets:
        ret.extend(partition_bucket(b, partition_thresh))
        processed += 1
        INFO('Processed {} buckets'.format(processed))
        if processed == 7:
            break
    return ret


def partition_huge_buckets_wrapper(inputfn, outputfn):
    buckets = jsonload(inputfn)
    partition_thresh = 0.15
    partitioned_buckets = partition_buckets(buckets, partition_thresh)
    text_summary_buckets(partitioned_buckets, outputfn)


def main():
    #  inputfn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__pos_samples.json'
    #  outputfn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__pos_samples_dist.json'
    #  csvoutputfn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__pos_dist_summary.csv'
    #  csvportionoutputfn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__pos_portion_dist_summary.csv'
    #  beta_fit_outputfn_pos = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__pos_beta_fit.json'
    #  beta_fit_csv_outputfn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__pos_beta_fit.csv'

    #  inputfn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__neg_samples.json'
    #  outputfn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__neg_samples_dist.json'
    #  csvoutputfn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__neg_dist_summary.csv'
    #  csvportionoutputfn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__neg_portion_dist_summary.csv'
    #  beta_fit_outputfn_neg = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__neg_beta_fit.json'
    #  beta_fit_csv_outputfn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__neg_beta_fit.csv'

    #  inputfn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_samples.json'
    #  outputfn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_samples_dist.json'
    #  csvoutputfn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_dist_summary.csv'
    #  csvportionoutputfn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_portion_dist_summary.csv'
    #  beta_fit_outputfn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_beta_fit.json'
    #  beta_fit_csv_outputfn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_beta_fit.csv'
    #  pos_buckets_fn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_pos_buckets.json'
    #  neg_buckets_fn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_neg_buckets.json'

    beta_fit_outputfn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__unlabeled_beta_fit.json'
    beta_fit_csv_outputfn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__unlabeled_beta_fit.csv'
    pos_buckets_fn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__unlabeled_pos_buckets.json'
    neg_buckets_fn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__unlabeled_neg_buckets.json'

    #  gen_dist_wrapper(inputfn, outputfn)
    #  summarize_dist_wrapper(outputfn, csvoutputfn)
    #  summarize_portion_dist_wrapper(outputfn, csvportionoutputfn)
    #  beta_fit_wrapper(outputfn, beta_fit_outputfn)
    beta_fit_csv_wrapper(beta_fit_outputfn, beta_fit_csv_outputfn)
    expectation_separator_wrapper(beta_fit_outputfn, pos_buckets_fn, neg_buckets_fn)

    #  pos_summary_fn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_pos_summary.txt'
    #  pos_summary_fn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__pos_pos_summary.txt'
    #  neg_summary_fn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_neg_summary.txt'
    #  neg_summary_fn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__neg_neg_summary.txt'
    pos_summary_fn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__unlabeled_pos_summary.txt'
    neg_summary_fn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__unlabeled_neg_summary.txt'
    text_summary(pos_buckets_fn, pos_summary_fn)
    text_summary(neg_buckets_fn, neg_summary_fn)

    #  whole_beta_fit_wrapper(beta_fit_outputfn)


if __name__ == '__main__':
    #  main()
    #  text_summary('/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_huge_clusters.json',
    #               '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_huge_clusters.txt')
    #  labeled_huge_cluster_parser('/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_labeled_huge_clusters.txt',
    #                              '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_labeled_huge_clusters.json')
    #  huge_cluster_similarity_stat_wrapper('/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_labeled_huge_clusters.json',
    #                                       '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_labeled_huge_cluster_inter.csv',
    #                                       '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_labeled_huge_cluster_intra.csv',)
    partition_huge_buckets_wrapper('/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_huge_clusters.json',
                                   '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__tot_huge_clusters_partitioned.txt')
