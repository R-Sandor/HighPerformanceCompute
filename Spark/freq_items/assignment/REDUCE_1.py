import sys

key = None
for cur_key in sys.stdin:
    if cur_key != key:
        print(cur_key)
    key = cur_key
