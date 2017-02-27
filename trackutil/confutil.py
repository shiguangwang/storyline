#!/usr/bin/env python

from definitions import CONFIG_FILE, PIPELINE_CONF_FILE
from trackutil.singleton import Singleton

import yaml


class ConfigSingleton:
    __metaclass__ = Singleton

    def __init__(self, conf_file):
        self.cfg = None
        self.conf_file = conf_file

    def get_config(self):
        if self.cfg is None:
            with open(self.conf_file, 'r') as cfg_file:
                self.cfg = yaml.load(cfg_file)
        return self.cfg


def get_config(conf_file=CONFIG_FILE):
    cfg = ConfigSingleton(conf_file)
    return cfg.get_config()


def get_pipeline_conf():
    cfg = ConfigSingleton(PIPELINE_CONF_FILE)
    return cfg.get_config()
