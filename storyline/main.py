#!/usr/bin/env python

import preprocess
import bucketize
import eventdetect

preprocess.main()
bucketize.bucketize()
eventdetect.main()
