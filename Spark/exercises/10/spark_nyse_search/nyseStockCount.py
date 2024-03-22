import sys
from operator import add
import re
# Dataframes are easier
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


if len(sys.argv) != 3:
    print >> sys.stderr, "Usage: wordcount <input_file_0> <output_file>"
    exit(-1)

spark = SparkSession.builder \
    .master("spark://hpd-master:7077") \
    .appName("NYSEStockCount") \
    .getOrCreate()

df = spark.read.option("header", True).csv(sys.argv[1])
df_filtered = df.filter(col("Name").contains("Technology") | col("Name").contains("Industrial") | col("Name").contains("United States"))
df_filtered.show()
# return only the symbols 
symbols = df_filtered.rdd.map(lambda row: (row["Symbol"], row["Name"]))

symbols.toDF(["Name"]).show()


symbols.saveAsTextFile(sys.argv[2])

spark.stop()
