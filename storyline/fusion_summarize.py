#!/usr/bin/env python

import os

from trackutil.confutil import get_config

from storyline.summarize import summarize


def main():
    cfg = get_config()
    root = cfg['data']['outdir']
    root = os.path.join(root, cfg['storyline']['datadir'])
    inputdir = os.path.join(root, cfg['storyline']['fusion']['datadir'])
    outputdir = os.path.join(root, cfg['storyline']['fusion_summarize']['datadir'])
    summarize(inputdir, outputdir)


if __name__ == '__main__':
    main()
