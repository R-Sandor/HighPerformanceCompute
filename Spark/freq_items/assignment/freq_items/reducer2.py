#!/usr/bin/env python

import sys

curr = None
prev = 0
c = 0

for ip in sys.stdin:
    ip = ip.strip()
    curr, value = ip.split(",", 1)
    value = int(value)
    if prev == curr:
        c = c + value
    else:
        if prev:
            if c >= 12:
                print("%s\t%d" % (prev, c))
        c = value
        prev = curr
if prev == curr:
    if c >= 12:
        print("%s\t%d" % (prev, c))
