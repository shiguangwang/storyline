#!/usr/bin/env python

from definitions import CONFIG_FILE
import yaml

with open(CONFIG_FILE, 'r') as cfgFile:
    cfg = yaml.load(cfgFile)


def get_data_srcdir():
    return cfg['data']['srcdir']


def get_data_outputdir():
    return cfg['data']['outputdir']
