from pyspark import SparkContext
import sys
if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: logmining <file>"
        exit(-1)
sc = SparkContext(appName="logmining")
text_file = sc.textFile(sys.argv[1])
errors = text_file.filter(lambda line: "error" in line)
errors.cache()
count = errors.count()
print("Total number of errors %d" % count)
browser_error = errors.filter(lambda line: "Mozilla" in line).count()
print("Mozilla error count :%d" % browser_error)
comptability_error = errors.filter(lambda line: "compatible" in line).count()
print("comptability error count :%d" % comptability_error)
iphone_error = errors.filter(lambda line: "iPhone" in line).count()
print("iphone error count :%d" % iphone_error)
