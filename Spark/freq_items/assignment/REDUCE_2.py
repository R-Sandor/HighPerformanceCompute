# Reduce Phase2 the final reduce. returning the the final dataset.
import sys

key = None
total = 0

for line in sys.stdin:
    line = line.strip()
    # Split on tabs
    cur_key, value = line.split("\t", 1)
    value = int(value)
    if key == cur_key:
        total += value
    else:
        if key:
            if total >= 12:
                print("%s\t%d" % (key, total))
        running_total = value
        key = cur_key
    if key == cur_key:
        if total >= 12:
            print("%s\t%d" % (key, total))
