from pyspark import SparkContext
import sys

# CS724
# Author: Raphael Sandor
# Pyspark Version Counting Triangle


def mapper(line):
    output = []
    adj = str(line).split()
    vertex = int(adj[0])
    neighbors = adj[1].split(",")
    neighbors = [int(i) for i in neighbors]

    for i in range(len(neighbors)):
        key = (vertex, neighbors[i])
        value = (key, [-1])
        output.append(value)

    for i in range(len(neighbors) - 1):
        for j in range((i + 1), len(neighbors)):
            key = (neighbors[i], neighbors[j])
            value = (key, [vertex])
            output.append(value)
    return output


def reduce(value):
    output = []
    verts = value[0]
    neighbors = list(set(value[1]))
    v1 = verts[0]
    v2 = verts[1]
    if -1 in neighbors:
        neighbors.remove(-1)
        for i in range(len(neighbors)):
            if neighbors[i] < v1 and neighbors[i] < v2:
                output.append((neighbors[i], v1, v2))
    return output


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("command: pyfile <input> <output>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="triangleCounting")
    data_rdd = sc.textFile(sys.argv[1])

    output = data_rdd.flatMap(mapper).reduceByKey(lambda x, y: x + y).flatMap(reduce)
    output.saveAsTextFile(sys.argv[2])
    sc.stop()
