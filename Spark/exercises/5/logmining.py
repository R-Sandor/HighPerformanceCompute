from pyspark import SparkContext
import sys
if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: logmining <file> <output>"
        exit(-1)
sc = SparkContext(appName="logmining")
text_file = sc.textFile(sys.argv[1])
errors = text_file.filter(lambda line: "error" in line)
errors.cache()
count = errors.count()
print("Total number of errors %d" % count)
browser_rdd = errors.filter(lambda line: "Mozilla" in line)
browser_error = browser_rdd.count()
print("Mozilla error count :%d" % browser_error)
comptability_rdd = errors.filter(lambda line: "compatible" in line)
comptability_error = comptability_rdd.count()
print("comptability error count :%d" % comptability_error)
iphone_rdd = errors.filter(lambda line: "iPhone" in line)
iphone_error = iphone_rdd.count()
print("iphone error count :%d" % iphone_error)

browser_rdd.union(comptability_rdd).union(iphone_rdd).saveAsTextFile(sys.argv[2])
