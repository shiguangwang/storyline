import os

from trackutil.confutil import get_config
from trackutil.ioutil import jsonload, jsondump
from trackutil.pathutil import mkdir, get_datafiles_in_dir
from trackutil.pathutil import get_storyline_module_dir
from trackutil.logger import INFO


def reformat_tbucket(tbucket):
    '''
    Reformat the original t-bucket to a general one.
    '''
    bucket = {
        'signature': tbucket[0],
        't_bucket': True,
        'i_bucket': False,
        'twitter': [],
        'instagram': [],
    }
    for tweet in tbucket[1]:
        bucket['twitter'].append([tweet[0], tweet[1]])
    return bucket


def reformat_wrapper(inputfn, outputfn):
    tbuckets = jsonload(inputfn)
    if tbuckets is None:
        return
    reformatted = []
    for tbucket in tbuckets:
        bucket = reformat_tbucket(tbucket)
        reformatted.append(bucket)
    jsondump(reformatted, outputfn)


def main():
    cfg = get_config()
    root = cfg['data']['outdir']
    root = os.path.join(root, cfg['storyline']['datadir'])
    inputdir = os.path.join(root, cfg['storyline']['purify']['datadir'])
    outputdir = os.path.join(root, cfg['storyline']['reformat']['datadir'])
    mkdir(outputdir)
    fnlist = get_datafiles_in_dir(inputdir)
    for fn in fnlist:
        inputfn = os.path.join(inputdir, fn)
        outputfn = os.path.join(outputdir, fn)
        reformat_wrapper(inputfn, outputfn)


def reformat_new(ts, cfg=None):
    INFO('[Reformat] {}'.format(ts))
    if cfg is None:
        cfg = get_config()
    inputdir = get_storyline_module_dir(cfg, 'purify')
    outputdir = get_storyline_module_dir(cfg, 'reformat')
    mkdir(outputdir)
    inputfn = os.path.join(inputdir, '{}.json'.format(ts))
    outputfn = os.path.join(outputdir, '{}.json'.format(ts))
    reformat_wrapper(inputfn, outputfn)


if __name__ == '__main__':
    main()
