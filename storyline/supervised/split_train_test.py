#!/usr/bin/env python

import math
import os

from random import shuffle
from shutil import copyfile

from trackutil.pathutil import mkdir, get_datafiles_in_dir
from trackutil.ioutil import jsondump, jsonload


def split(inputdir, traindir, testdir, train_ratio):
    mkdir(traindir)
    mkdir(testdir)
    fn_list = get_datafiles_in_dir(inputdir)
    shuffle(fn_list)
    total = len(fn_list)
    trainlen = int(math.ceil(total * train_ratio))
    for i in range(trainlen):
        copyfile(
            os.path.join(inputdir, fn_list[i]),
            os.path.join(traindir, fn_list[i]))
    for i in range(trainlen, total):
        copyfile(
            os.path.join(inputdir, fn_list[i]),
            os.path.join(testdir, fn_list[i]))


def claspe(datadir, outputfn):
    fn_list = get_datafiles_in_dir(datadir)
    biglist = []
    for fn in fn_list:
        buckets = jsonload(os.path.join(datadir, fn))
        biglist.append(buckets.items())
    jsondump(biglist, os.path.join(datadir, outputfn))


def main():
    inputdir = '/home/shiguang/Projects/evtrack/data/storyline/summarize'
    traindir = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train'
    testdir = '/home/shiguang/Projects/evtrack/data/storyline/supervised/test'
    train_ratio = 0.8
    split(inputdir, traindir, testdir, train_ratio)
    claspefn = '__clasped.json'
    claspe(traindir, claspefn)


if __name__ == '__main__':
    main()
