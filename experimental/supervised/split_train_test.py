#!/usr/bin/env python

import math
import os

from random import shuffle
from shutil import copyfile

from trackutil.pathutil import mkdir, get_datafiles_in_dir
from trackutil.ioutil import jsondump, jsonload


def sample(inputdir, outputdir=None, sample_interval=24):
    '''
    We sample the file in a dir one outof sample_interval and maintain the
    order of the file in the dir, since they are sorted by time.
    '''
    if outputdir is None:
        return
    mkdir(outputdir)
    fn_list = get_datafiles_in_dir(inputdir)
    for i in range(0, len(fn_list), sample_interval):
        copyfile(
            os.path.join(inputdir, fn_list[i]),
            os.path.join(outputdir, fn_list[i]))


def shuffle_split(inputdir, traindir=None, testdir=None, train_ratio=0.8):
    '''
    Shuffle the file list in a dir, and split it into train and test sets as
    defined by the train_ratio, i.e. the number of files in the train set is
    the total number of files in the list multiply the train_ratio.

    Note that if the traindir is None, then we simply skip this function,
    while if the testdir is None, then we skip the test files.
    '''
    if traindir is None:
        return
    mkdir(traindir)
    fn_list = get_datafiles_in_dir(inputdir)
    shuffle(fn_list)
    total = len(fn_list)
    trainlen = int(math.ceil(total * train_ratio))
    for i in range(trainlen):
        copyfile(
            os.path.join(inputdir, fn_list[i]),
            os.path.join(traindir, fn_list[i]))
    if testdir is None:
        return
    mkdir(testdir)
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
    datadir = '/home/shiguang/Projects/evtrack/data/storyline/unite_test'
    sampledir = '/home/shiguang/Projects/evtrack/data/experimental/samples'
    sample(datadir, sampledir)
    traindir = '/home/shiguang/Projects/evtrack/data/experimental/train'
    testdir = None
    train_ratio = 1.0
    shuffle_split(sampledir, traindir, testdir, train_ratio)
    claspefn = '__clasped.json'
    claspe(traindir, claspefn)


if __name__ == '__main__':
    main()
