#!/usr/bin/env python

import os

from definitions import ROOT_DIR
from trackutil.confutil import get_config


def get_filenames_in_dir(input_dir):
    """ Get the filename list of a dir """
    filenames = [f for f in os.listdir(input_dir)
                 if os.path.isfile(os.path.join(input_dir, f))]
    return filenames


def get_datafiles_in_dir(d):
    ''' Get all the data files in a dir. Specific to our dataset '''
    filenames = get_filenames_in_dir(d)
    # do some heuristic processing for our dataset
    filenames = [f for f in filenames
                 if not f.startswith('timestamps') and
                 not f.startswith('.') and
                 not f.startswith('__')]
    filenames = sorted(filenames, key=lambda x: int(x.split('.')[0]))
    return filenames


def get_timestamps_in_dir(d):
    ''' Get all the timestamps in data file names in a dir.'''
    filenames = get_datafiles_in_dir(d)
    filenames = [f.split('.')[0] for f in filenames]
    return filenames


def get_ts_int_in_dir(d):
    tslist = get_timestamps_in_dir(d)
    tslist = [int(t) for t in tslist]
    return tslist


def get_obsolute_path(d1, *ds):
    ''' Get the obsolute path of one of a bunch of subsequent paths '''
    cfg = get_config()
    root = cfg['data']['outdir']
    res = os.path.join(root, d1)
    for d in ds:
        res = os.path.join(res, d)
    return res


def get_storyline_root(cfg):
    root = cfg['data']['outdir']
    root = os.path.join(root, cfg['storyline']['datadir'])
    return root


def get_storyline_module_dir(cfg, key):
    root = get_storyline_root(cfg)
    cur_dir = cfg['storyline'][key]['datadir']
    return os.path.join(root, cur_dir)


def mkdir(d):
    ''' Create the directory if it does not exist '''
    if not os.path.exists(d):
        os.makedirs(d)


def strip_root(p):
    ''' Output the relative path of the project root of the input path '''
    return "@/" + os.path.relpath(p, ROOT_DIR)
