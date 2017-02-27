#!/usr/bin/env python

from trackutil.ioutil import jsonload, jsondump


def unify_label(datafn):
    data = jsonload(datafn)
    total = sum([len(item) for item in data])
    labeled = 0
    stop = False
    for i in range(len(data)):
        if stop:
            break
        buckets = data[i]
        for bucket in buckets:
            if len(bucket) > 2:
                # means that we have labeled it already
                labeled += 1
                continue
            print "\n\nLabeled {0}/{1}, {2}% finished.".format(
                labeled, total, labeled * 100.0 / total)
            labeled += 1
            print "Key: {}\n".format(bucket[0])
            for tweet in bucket[1]['tweets']:
                print "\t*** {}".format(tweet[1].encode('ascii', 'ignore'))
            print '\n'
            inp = raw_input("t/f/s >>> ").strip()
            while inp != 't' and inp != 'f' and inp != 's':
                inp = raw_input("t/f/s >>> ").strip()
            if inp == 't':
                bucket.append(True)
                print 'You labeled it True'
            elif inp == 'f':
                bucket.append(False)
                print 'You labeled it False'
            else:
                stop = True
                print 'Seems you are tired of this. Have a good break!'
                break
    jsondump(data, datafn)


def purify_label(datafn):
    data = jsonload(datafn)
    total = sum([len(item) for item in data])
    labeled = 0
    stop = False
    for i in range(len(data)):
        if stop:
            break
        buckets = data[i]
        for bucket in buckets:
            if len(bucket) > 2:
                # means that we have labeled it already
                labeled += 1
                continue
            print "\n\nLabeled {0}/{1}, {2}% finished.".format(
                labeled, total, labeled * 100.0 / total)
            labeled += 1
            print "Key: {}\n".format(bucket[0])
            for tweet in bucket[1]:
                print "\t*** {}".format(tweet[1].encode('ascii', 'ignore'))
            print '\n'
            inp = raw_input("t/f/s >>> ").strip()
            while inp != 't' and inp != 'f' and inp != 's':
                inp = raw_input("t/f/s >>> ").strip()
            if inp == 't':
                bucket.append(True)
                print 'You labeled it True'
            elif inp == 'f':
                bucket.append(False)
                print 'You labeled it False'
            else:
                stop = True
                print 'Seems you are tired of this. Have a good break!'
                break
    jsondump(data, datafn)


def main():
    #  datafn = '/home/shiguang/Projects/evtrack/data/experimental/train/__clasped.json'
    consolidated_datafn = '/home/shiguang/Projects/evtrack/data/storyline/supervised/train/__clasped.json'
    purify_label(consolidated_datafn)


if __name__ == '__main__':
    main()
