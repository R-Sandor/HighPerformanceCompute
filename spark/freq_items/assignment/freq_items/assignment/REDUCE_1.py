import sys
new_pair = None
for pair in sys.stdin:
    if pair != new_pair:
        new_pair = pair
        print(pair)
		
