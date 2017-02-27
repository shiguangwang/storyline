#!/usr/bin/env python

import os

from trackutil.confutil import get_config
from trackutil.ioutil import jsonload
from trackutil.logger import INFO
from trackutil.pathutil import get_datafiles_in_dir, mkdir
from trackutil.pathutil import get_storyline_module_dir


def main():
    cfg = get_config()
    root = cfg['data']['outdir']
    root = os.path.join(root, cfg['storyline']['datadir'])
    inputdir = os.path.join(root, cfg['storyline']['reformat']['datadir'])
    outputdir = os.path.join(root, cfg['storyline']['summarize']['datadir'])
    summarize(inputdir, outputdir)


def text_summary(inputfn, outputfn):
    buckets = jsonload(inputfn)
    if buckets is None:
        return
    text_summary_buckets(buckets, outputfn)


def text_summary_buckets(buckets, outputfn):
    processed = 0
    with open(outputfn, 'w') as f:
        for b in buckets:
            processed += 1
            f.write('==================================\n')
            f.write('{0}. {1}\n'.format(processed, b['signature']))
            if b['i_bucket'] and b['t_bucket']:
                bucket_type_str = 'I-bucket and T-bucket'
            elif b['i_bucket']:
                bucket_type_str = 'I-bucket'
            elif b['t_bucket']:
                bucket_type_str = 'T-bucket'
            else:
                bucket_type_str = 'Error'
            f.write('Bucket type: {}\n'.format(bucket_type_str))
            if 'likelihood' in b:
                f.write('Likelihood: {}\n'.format(b['likelihood']))
            f.write('\n')
            for t in b['twitter']:
                f.write('\t*** <{}> {}\n'.format(
                    t[0], t[1].encode('utf-8', 'ignore')))
            if len(b['instagram']) > 0:
                f.write('\n\t\t----------Instagram----------\n\n')
            for i in b['instagram']:
                f.write('\t+++ ({}) [{}]\n'.format(
                    b['instagram'].index(i),
                    i['name'].encode('utf-8', 'ignore')))
                f.write('\t\t tags: {}\n'.format(
                    (', '.join(i['tags']).encode('utf-8', 'ignore'))))
                f.write('\t\t url: {}\n'.format(
                    i['url'].encode('utf-8', 'ignore')))
            f.write('\n')


def summarize(inputdir, outputdir):
    mkdir(outputdir)
    fn_list = get_datafiles_in_dir(inputdir)
    processed = 0
    total = len(fn_list)
    #  fnlist = ['{}.json'.format(fn) for fn in range(610, 626)]
    for fn in fn_list:
        inputfn = os.path.join(inputdir, fn)
        outfn = fn[:-4] + 'txt'
        outputfn = os.path.join(outputdir, outfn)
        text_summary(inputfn, outputfn)
        processed += 1
        if processed % 10 == 0:
            INFO("summarized {0}/{1}".format(processed, total))


def summarize_new(ts, input_module, output_module, cfg=None):
    INFO('[Tbucket Summarize] {}'.format(ts))
    if cfg is None:
        cfg = get_config()
    inputdir = get_storyline_module_dir(cfg, input_module)
    outputdir = get_storyline_module_dir(cfg, output_module)
    mkdir(outputdir)
    inputfn = os.path.join(inputdir, '{}.json'.format(ts))
    outputfn = os.path.join(outputdir, '{}.txt'.format(ts))
    text_summary(inputfn, outputfn)


if __name__ == "__main__":
    main()
    #  inputfn = '/home/shiguang/Projects/evtrack/data/workshop/storyline/absorb/610.json'
    #  outputfn = '/home/shiguang/absorbed_610.txt'
    #  text_summary(inputfn, outputfn)
