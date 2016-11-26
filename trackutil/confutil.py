#!/usr/bin/env python

from definitions import CONFIG_FILE
from trackutil.singleton import Singleton

import yaml


class ConfigSingleton:
    __metaclass__ = Singleton

    def __init__(self):
        self.cfg = None

    def get_config(self):
        if self.cfg is None:
            with open(CONFIG_FILE, 'r') as cfg_file:
                self.cfg = yaml.load(cfg_file)
        return self.cfg


def get_config():
    cfg = ConfigSingleton()
    return cfg.get_config()
