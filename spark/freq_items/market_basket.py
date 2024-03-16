from pyspark import SparkContext

sc = SparkContext(appName="test")
from operator import add
from numpy import array
import sys
import itertools

if len(sys.argv) != 2:
    print >> sys.stderr, "Usage: market_basket <input_file>"
    exit(-1)
data = sc.textFile(sys.argv[1])
data1 = data.map(lambda line: ([int(x) for x in line.strip().split(" ")]))
comb = data1.flatMap(lambda line: itertools.combinations(line, 2))
comb_key = comb.map(lambda x: (x, 1)).reduceByKey(add)
comb_fil = comb_key.filter(lambda kv: kv[1] >= 12)
print(comb_fil.collect())
