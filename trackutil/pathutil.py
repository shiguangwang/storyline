#!/usr/bin/env python

import os

from definitions import ROOT_DIR
from trackutil.confutil import get_config


def get_filenames_in_dir(input_dir):
    """ Get the filename list of a dir """
    filenames = [f for f in os.listdir(input_dir)
                 if os.path.isfile(os.path.join(input_dir, f))]
    filenames.sort()
    return filenames


def get_datafiles_in_dir(d):
    ''' Get all the data files in a dir. Specific to our dataset '''
    filenames = get_filenames_in_dir(d)
    # do some heuristic processing for our dataset
    filenames = [f for f in filenames
                 if not f.startswith('timestamps') and
                 not f.startswith('.') and
                 not f.startswith('__')]
    return filenames


def get_obsolute_path(d1, *ds):
    ''' Get the obsolute path of one of a bunch of subsequent paths '''
    cfg = get_config()
    root = cfg['data']['outdir']
    res = os.path.join(root, d1)
    for d in ds:
        res = os.path.join(res, d)
    return res


def mkdir(d):
    ''' Create the directory if it does not exist '''
    if not os.path.exists(d):
        os.makedirs(d)


def strip_root(p):
    ''' Output the relative path of the project root of the input path '''
    return "@/" + os.path.relpath(p, ROOT_DIR)
