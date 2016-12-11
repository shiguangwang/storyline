#!/usr/bin/env python
import os

from trackutil.confutil import get_config
from trackutil.pathutil import get_timestamps_in_dir


def main():
    '''
    This module aims to build a commandline interface for browsing the events
    The idea is similar as an event loop.
    '''
    cfg = get_config()
    root = cfg['data']['outdir']
    root = os.path.join(root, cfg['storyline']['datadir'])
    root = os.path.join(root, cfg['storyline']['consolidate']['datadir'])
    help_str = '''
Storyline Commandline Interface:
    - (t)imestamp to list and select a timestamp from the ts list
    - (q)uit to quit the program
    - (h)elp to display this message
'''
    while True:
        print help_str
        #  cmd = sys.stdin.readline('>>').strip()
        cmd = raw_input('0 >> ')
        if cmd == 'help' or cmd == 'h':
            continue
        if cmd == 'quit' or cmd == 'q':
            print "bye"
            break
        if cmd == 'timestamp' or cmd == 't':
            ts(root)


def _gen_ts_str(idx, block, ts_list):
    ts_str = 'Timestamp listing {0} - {1} / {2}'.format(
        idx, idx + block - 1, len(ts_list))
    for i in range(idx, min(idx + block, len(ts_list))):
        ts_str += '''
    ({0}) {1}'''.format(i - idx, ts_list[i])
    return ts_str


class Selector:
    '''
    The Selector class is for browsing the objects in a list.
    It takes a callback function which defines the operation for each item
    in the list.
    '''
    def __init__(self, content_list, app_name='App', content_cb=None):
        self.app_name = app_name
        self.content_list = content_list
        self.content_cb = content_cb
        self.block = 10
        self.help_str = '''
Select a {} to proceed:
    - [num]: make a selection
    - (r)eset: display from the first index
    - (n)ext: display next page
    - (m)ore: jump to the next 5 pages
    - (p)rev: display the previous page
    - (b)ack: jump to the previous 5 pages
    - (q)uit: goto uplevel
    - (h)elp: display this message

'''.format(self.app_name)
        self.quit_str = '''
        Goto the uplevel.
'''

    def _gen_content_str(self, idx):
        '''
        Generate the string showing the position of the current view.
        '''
        content_str = '{3} listing {0} - {1} / {2}'.format(
            idx, idx + self.block - 1, len(self.content_list), self.app_name)
        for i in range(idx, min(idx + self.block, len(self.content_list))):
            content_str += '''
        ({0}) {1}'''.format(i - idx, self.content_list[i])
        return content_str

    def exe(self):
        '''
        The function actually executes everything.
        '''
        idx = 0
        print_help = True
        while True:
            if print_help:
                print self.help_str
                print_help = False
            print self._gen_content_str(idx)
            cmd = raw_input('t >> ')
            print cmd
            if cmd.isdigit():
                i = int(cmd)
                if i < 0 or i > 9:
                    print_help = True
                else:
                    self.content_cb(idx + i)
            elif cmd == 'reset' or cmd == 'r':
                idx = 0
            elif cmd == 'next' or cmd == 'n':
                if idx + self.block < len(self.content_list):
                    idx += self.block
            elif cmd == 'more' or cmd == 'm':
                if idx + self.block * 5 < len(self.content_list):
                    idx += self.block * 5
            elif cmd == 'prev' or cmd == 'p':
                print "previous"
                if idx >= self.block:
                    idx -= self.block
            elif cmd == 'back' or cmd == 'b':
                if idx >= self.block * 5:
                    idx -= self.block * 5
            elif cmd == 'quit' or cmd == 'q':
                print self.quit_str
                break
            else:
                print_help = True


def ts(inputdir):
    '''
    The interface for handling the timestamps in an inputdir.
    '''
    ts_list = get_timestamps_in_dir(inputdir)

    def content_cb(idx):
        '''
        The callback to handle each timestamp.
        TODO Adding the browsing function for the tweets under this timestamp.
        '''
        print ts_list[idx]

    selector = Selector(ts_list, app_name='timestamp', content_cb=content_cb)
    selector.exe()


if __name__ == '__main__':
    main()
