#!/usr/bin/env python

import csv
import os
import sys

from trackutil.ioutil import jsonload
from trackutil.pathutil import get_datafiles_in_dir


def track_length():
    inputdir = sys.argv[1]
    fnlist = get_datafiles_in_dir(inputdir)
    fnlist = [f for f in fnlist if 'json' in f]
    seqlist = [int(f.split('.')[0]) for f in fnlist]
    seqlist = sorted(seqlist)
    len_list = []
    for seq in seqlist:
        fn = '{}.json'.format(seq)
        event = jsonload(os.path.join(inputdir, fn))
        len_list.append(len(event['windows']))
    csv_fn = '__track_len.csv'
    with open(os.path.join(inputdir, csv_fn), 'wb') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(len_list)


if __name__ == '__main__':
    track_length()
