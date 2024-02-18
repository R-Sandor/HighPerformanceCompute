import sys
from operator import add
import re
# Using SparkContext rather than session to get the RDD. 
from pyspark import SparkContext

if len(sys.argv) != 3:
    print >> sys.stderr, "Usage: wordcount <input_file_0>  <output_file>"
    exit(-1)

sc = SparkContext(appName="Wordcount")
rdd = sc.textFile(sys.argv[1])

word_count = rdd.flatMap(lambda line:  re.split('\W+', line)) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(add)

print('Top 10 lines: %s' % word_count.takeSample(0, 10)) 


word_count.saveAsTextFile(sys.argv[2])

sc.stop()
