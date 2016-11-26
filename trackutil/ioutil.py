#!/usr/bin/env python

import json

from trackutil.logger import DEBUG


def jsondump(obj, fn):
    ''' Dump an object to a file in json format '''
    try:
        with open(fn, 'w') as f:
            json.dump(obj, f)
        return True
    except IOError, e:
        DEBUG('errno {0} {1}: {2}'.format(e.errno, e.strerror, e.filename))
    return False


def jsonload(fn):
    ''' Load the object of json format from a file '''
    try:
        with open(fn, 'r') as f:
            return json.load(f)
    except ValueError, e:
        DEBUG(e.message)
    except IOError, e:
        DEBUG("errno {0} {1}: {2}".format(e.errno, e.strerror, e.filename))
    return None
