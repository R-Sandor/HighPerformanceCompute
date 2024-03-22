# Map Phase2, called after reducing the apriori keys.
import sys

# Declare the datasets
ds_1 = []
ds_2 = []

# Read the keys
for l1 in sys.stdin:
    l1 = l1.strip()
    i1 = l1.split(" ")
    ds_1.append(i1)
ds_1 = list(map(set, ds_1))

# Read in the first part of the data
for l2 in open("part-00000", "r"):
    l2 = l2.strip()
    if l2:
        i2 = l2.split(" ")
        ds_2.append(i2)
# Freeze the set
ds_2 = list(map(frozenset, ds_2))

# Return stream for next phase.
for i in ds_1:
    for j in ds_2:
        if j.issubset(i):
            x,y=j
            print("{} {},1".format(x,y))
