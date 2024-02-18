import sys
from operator import add
from pyspark import SparkContext


if len(sys.argv) != 3:
    print >> sys.stderr, "Usage: wordcount <input_file> <output_file>"
    exit(-1)
sc = SparkContext(appName="PythonWordCount")
lines = sc.textFile(sys.argv[1])
counts = lines.flatMap(lambda line: line.strip().split(' ')).map(lambda word: (word, 1)).reduceByKey(add)
counts.saveAsTextFile(sys.argv[2])
sc.stop()
