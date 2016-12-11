#!/usr/bin/env python

import json
import os

from trackutil.confutil import get_config
from trackutil.ioutil import jsonload
from trackutil.logger import INFO
from trackutil.pathutil import get_datafiles_in_dir, mkdir


def main():
    cfg = get_config()
    root = cfg['data']['outdir']
    root = os.path.join(root, cfg['storyline']['datadir'])
    inputdir = os.path.join(root, cfg['storyline']['consolidate']['datadir'])
    outputdir = os.path.join(root, cfg['storyline']['summarize']['datadir'])
    summarize(inputdir, outputdir)


#  def summarize_bucket(buckets):
#      ret = ''
#      for kwp in buckets:
#          tweets = buckets[kwp]
#          ret += '# {0}\n'.format(kwp)
#          for tweet in tweets:
#              ret += '* [{0}] {1}\n'.format(
#                  tweet[2], tweet[1].encode('ascii', errors='ignore'))
#          ret += '\n'
#      return ret


def summarize_bucket(buckets):
    ret = json.dumps(buckets, indent=2, sort_keys=True)
    return ret


def summarize(inputdir, outputdir):
    mkdir(outputdir)
    fn_list = get_datafiles_in_dir(inputdir)
    processed = 0
    total = len(fn_list)
    step = 6
    for i in range(0, total, step):
        fn = fn_list[i]
        buckets = jsonload(os.path.join(inputdir, fn))
        summary = summarize_bucket(buckets)
        outfn = fn[:-4] + 'txt'
        with open(os.path.join(outputdir, outfn), 'w') as f:
            f.write(summary)
        f.close()
        processed += 1
        if processed % 10 == 0:
            INFO("summarized {0}/{1}".format(processed, total))


if __name__ == "__main__":
    main()
