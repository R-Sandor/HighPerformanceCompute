# Reduce Phase2 the final reduce. returning the the final dataset.
import sys

last = None
current = None
total = 0

# Read all the keys in from the stream
for line in sys.stdin:
    line = line.strip()
    current, v = line.split(",", 1)
    v = int(v)
    if last == current:
        total += v
    else:
        if last:
            if total >= 12:
                print("{}\t{:n}".format(last, total))
        total = v
        last = current 

if last == current:
    if total >= 12:
        print("{}\t{:n}".format(last_key, total))

