#!/usr/bin/env python

import os

from trackutil.confutil import get_config
from trackutil.ioutil import jsonload, jsondump
from trackutil.pathutil import mkdir
from trackutil.logger import DEBUG


def get_event_id_fn():
    cfg = get_config()
    rootdir = cfg['data']['outdir']
    mkdir(rootdir)
    rootdir = os.path.join(rootdir, cfg['storyline']['datadir'])
    mkdir(rootdir)
    event_id_fn = os.path.join(rootdir, cfg['storyline']['event_id_gen_fn'])
    return event_id_fn


def grab_event_id():
    event_id_fn = get_event_id_fn()
    event_id = 0
    if os.path.isfile(event_id_fn):
        event_id = jsonload(event_id_fn)
        DEBUG('Grab last event id {}'.format(event_id))
        event_id += 1
    jsondump(event_id, event_id_fn)
    return event_id


def reset_event_id():
    '''
    This will remove the event_id globally.
    Use with caution.
    '''
    event_id_fn = get_event_id_fn()
    os.remove(event_id_fn)


if __name__ == '__main__':
    print grab_event_id()
    print grab_event_id()
    print grab_event_id()
    print grab_event_id()
    print grab_event_id()
    print grab_event_id()
    print grab_event_id()
    print grab_event_id()
    reset_event_id()

    print grab_event_id()
    print grab_event_id()
    print grab_event_id()
    print grab_event_id()
    reset_event_id()
