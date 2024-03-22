from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.ml.classification import LogisticRegression
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql import SparkSession
from pyspark import SparkContext
from numpy import array
import sys
# Load and parse the data
def parsePoint(line):
    values = [float(x) for x in line.replace(',', ' ').split(' ')]
    return LabeledPoint(values[0], values[1:])


if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: logistic_regression <file>"
        exit(-1)

sc = SparkContext(appName="PythonLR")
data = sc.textFile(sys.argv[1])
parsedData = data.map(parsePoint)

lr = LogisticRegressionWithSGD.train(parsedData)

# Print the coefficients (final weights)
labelsAndPreds = parsedData.map(lambda p: (p.label, lr.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda row: row[0] != row[1]).count() / float(parsedData.count())
final_weights = lr.weights
print("Final Weights (Vector w):", final_weights)

sc.stop()
