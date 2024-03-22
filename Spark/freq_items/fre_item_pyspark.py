from pyspark import SparkContext
from operator import add
import sys

sc = SparkContext(appName="fre_item")


def createC1(dataset):
    "Create a list of candidate item sets of size one."
    c1 = []
    for transaction in dataset:
        for item in transaction:
            if [item] not in c1:
                c1.append([item])
    c1.sort()
    # print("C1: {}".format(c1))
    # Convert to a list of frozensets
    return list(map(frozenset, c1))


def scanD(dataset, candidates, min_support):
    "Returns all candidates that meets a minimum support level"
    # print("Entering for loop")
    sscnt = {}
    for tid in dataset:
        # print("tid: {}".format(tid))
        for can in candidates:
            # print("can: {}".format(can))
            if can.issubset(tid):
                sscnt.setdefault(can, 0)
                sscnt[can] += 1

    retlist = []
    support_data = {}
    for key in sscnt:
        support = sscnt[key]
        if support >= min_support:
            retlist.insert(0, key)
        support_data[key] = support
    # return retlist, support_data
    return retlist


def aprioriGen(freq_sets, k):
    "Generate the joint transactions from candidate sets"
    retList = []
    lenLk = len(freq_sets)
    for i in range(lenLk):
        for j in range(i + 1, lenLk):
            L1 = list(freq_sets[i])[: k - 2]
            L2 = list(freq_sets[j])[: k - 2]
            L1.sort()
            L2.sort()
            if L1 == L2:
                retList.append(freq_sets[i] | freq_sets[j])
    return retList


def transform_function(rdd):
    dataset = rdd.collect()
    # Get the counts of each item's occurrence in baskets
    rdd1_c1 = createC1(dataset)
    # Create a set for each bucket
    dataset = list(map(set, dataset))
    # print(list(dataset))
    # Provide that set to scanD that returns all the candidates
    f = scanD(dataset, rdd1_c1, 3)
    # print("f {}".format(f))
    dual_1 = aprioriGen(f, 2)
    L2 = scanD(dataset, dual_1, 3)
    a = []
    for i in dataset:
        for j in L2:
            if j.issubset(i):
                a.append([j, 1])
    a = sc.parallelize(a)
    a = a.reduceByKey(add)
    return a


if len(sys.argv) != 2:
    print(sys.stderr, "Usage: fre_tem_pyspark <input_file>")
    exit(-1)

data = sc.textFile(sys.argv[1])
data1 = data.map(lambda line: ([int(x) for x in line.split(" ")]))
rdd1, rdd2 = data1.randomSplit([10, 10], 0)
print("rdd1 before {}".format(rdd1.collect()))
print("rdd2 before {}".format(rdd2.collect()))
print("TRANSFORMING")
transformed_rdd1 = transform_function(rdd1)
print("rrd1 {}".format(transformed_rdd1.collect()))
print()
transformed_rdd2 = transform_function(rdd2)
print("rrd2 {}".format(transformed_rdd2.collect()))
print()
total = transformed_rdd2.join(transformed_rdd1)
print()
print("combined {}".format(total.collect()))

final = total.map(lambda kvw: (kvw[0], kvw[1][0] + kvw[1][1]))
result = final.filter(lambda kv: kv[1] >= 12)

print()
print("FINAL RESULT")
print(result.collect())
