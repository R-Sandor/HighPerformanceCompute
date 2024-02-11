import sys
from operator import add
import re
# Using SparkContext rather than session to get the RDD. 
from pyspark import SparkContext

if len(sys.argv) != 4:
    print >> sys.stderr, "Usage: wordcount <input_file_0> <input_file_1> <output_file>"
    exit(-1)
sc = SparkContext(appName="PythonWordCount")
readme_rdd = sc.textFile(sys.argv[1])
contributing_rdd = sc.textFile(sys.argv[2])
rdd = readme_rdd.union(contributing_rdd)

spark_count = rdd.flatMap(lambda line:  re.split('\W+', line)) \
        .filter(lambda word: word == "Spark") \
        .map(lambda word: (word, 1)) \
        .reduceByKey(add)


print('Total Sparks Counted in %s & %s: %s' % (sys.argv[1], sys.argv[2], spark_count.collect()))
spark_count.saveAsTextFile(sys.argv[3])

sc.stop()
