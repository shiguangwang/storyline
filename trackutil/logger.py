#!/usr/bin/env python

import numbers
import sys

import inspect

from datetime import datetime
from trackutil.singleton import Singleton
from trackutil.pathutil import strip_root


class Log_:
    '''
    Please do not use this class directly. Instead using the following functions
    '''
    __metaclass__ = Singleton

    def __init__(self):
        self.TIME_FMT = "%Y-%m-%d %H:%M:%S"
        self.CRITICAL = {i[0]: i[1] for i in enumerate(
            ['D', 'I', 'W', 'E'])}
        self.level_ = 1  # the default is INFO

    def set_level(self, level):
        if isinstance(level, numbers.Number):
            if level >= len(self.CRITICAL):
                self.level_ = 3  # set to the ERROR
            elif level < 0:
                self.level_ = 0  # set to the DEBUG
            else:
                self.level_ = level
        else:
            if level.lower() == 'debug':
                self.level_ = 0
            elif level.lower() == 'info':
                self.level_ = 1
            elif level.lower() == 'warning':
                self.level_ = 2
            elif level.lower() == 'error':
                self.level_ = 3
            else:
                self.level_ = 1  # set to the default INFO level

    def log_(self, critical, msg, fn, ln):
        if self.level_ <= critical:
            print >> sys.stderr, "[{c}] {f}:{l} [{t}] - {m}".format(
                c=self.CRITICAL[critical],
                t=datetime.now().strftime(self.TIME_FMT),
                f=fn, l=ln, m=msg)

    def debug(self, msg, fn, ln):
        self.log_(0, msg, fn, ln)

    def info(self, msg, fn, ln):
        self.log_(1, msg, fn, ln)

    def warning(self, msg, fn, ln):
        self.log_(2, msg, fn, ln)

    def error(self, msg, fn, ln):
        self.log_(3, msg, fn, ln)


def SET_LOG_LEVEL(level):
    log = Log_()
    log.set_level(level)


def DEBUG(msg):
    (_, fn, ln, _, _, _) = inspect.getouterframes(inspect.currentframe())[1]
    fn = strip_root(fn)
    log = Log_()
    log.debug(msg, fn, ln)


def INFO(msg):
    (_, fn, ln, _, _, _) = inspect.getouterframes(inspect.currentframe())[1]
    fn = strip_root(fn)
    log = Log_()
    log.info(msg, fn, ln)


def WARNING(msg):
    (_, fn, ln, _, _, _) = inspect.getouterframes(inspect.currentframe())[1]
    fn = strip_root(fn)
    log = Log_()
    log.warning(msg, fn, ln)


def ERROR(msg):
    (_, fn, ln, _, _, _) = inspect.getouterframes(inspect.currentframe())[1]
    fn = strip_root(fn)
    log = Log_()
    log.error(msg, fn, ln)


def LOG(level, msg):
    (_, fn, ln, _, _, _) = inspect.getouterframes(inspect.currentframe())[1]
    fn = strip_root(fn)
    log = Log_()
    if level == 'INFO':
        log.info(msg, fn, ln)
    elif level == 'WARNING':
        log.warning(msg, fn, ln)
    elif level == 'DEBUG':
        log.debug(msg, fn, ln)
    elif level == 'ERROR':
        log.error(msg, fn, ln)
    else:
        ERROR("Log level unrecognized")
