import re
import sys
from operator import add
from pyspark import SparkContext

sc = SparkContext(appName="PythonPageRank")


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r"\s+", urls)
    return parts[0], parts[1]


if len(sys.argv) != 5:
    print(
        "Usage: pagerank.py <input_file> <input_file_2> <output_file> <outputfile_2> <number of iterations>",
        file=sys.stderr,
    )
    exit(-1)

lines = sc.textFile(sys.argv[1], 1)
pages = sc.textFile(sys.argv[2], 1)

print("outputs for inital run for understanding")
links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
pagelinks = pages.map(lambda urls: parseNeighbors(urls)).cache()

for link, neighs in links.collect():
    print("\n{} has neigbors (directed): ".format(link))
    for neigh in neighs:
        print(" {} ".format(neigh))
ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

print("\n\n***setting up initial rank to 1.0***")

for page, rank in ranks.collect():
    print("\n{} has initial rank: {}".format(page, rank))
temp_join = links.join(ranks)
contribs = temp_join.flatMap(
    lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1])
)

print(
    "\n***calculating contirbutions for each page from nebhiours by rank/number of links going out of the page\n***"
)

for page, contrib in contribs.collect():
    print(
        "page {} is getting contribution of {} from its neighbor".format(page, contrib)
    )

temp_aggreg = contribs.reduceByKey(add)

print("\n*** adding all the contirbutions****\n")

for page, aggreg in temp_aggreg.collect():
    print(
        "page {} has aggregated contribution of {} from all its neighbor".format(
            page, aggreg
        )
    )

print(
    "\n*** recalculating ranks using damping factor and guessing iniatial PR as 0****\n"
)

temp_ranks = temp_aggreg.mapValues(lambda rank: rank * 0.85 + 0.15)

for page, rank in temp_ranks.collect():
    print("page {} has an updated rank of {}".format(page, rank))

print("\n*** doing the same steps for given number of iterations ***\n")
# Use the min/max function that are based on the keys for an RDD
# Flip the key values
pgRank = temp_ranks.map(lambda x: (x[1], x[0]))

min = pgRank.min()
max = pgRank.max()
for iteration in range(int(sys.argv[4])):
    print("\n*** Iteration %d***" % iteration)

    contribs = links.join(ranks).flatMap(
        lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1])
    )

    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    reversed = ranks.map(lambda x: (x[1], x[0]))
    min = reversed.min()
    max = reversed.max()
    print(
        "Min ranked {}, value: {}, URL: {}".format(
            min[1], min[0], pagelinks.lookup(min[1])
        )
    )
    print(
        "Max ranked {}, value: {}, URL: {}".format(
            max[1], max[0], pagelinks.lookup(max[1])
        )
    )
    for link, rank in ranks.collect():
        print("%s has rank: %s." % (link, rank))

print(
    "Min ranked {}, value: {}, URL: {}".format(min[1], min[0], pagelinks.lookup(min[1]))
)
print(
    "Max ranked {}, value: {}, URL: {}".format(max[1], max[0], pagelinks.lookup(max[1]))
)


ranks.repartition(1).saveAsTextFile(sys.argv[3])
sc.stop()
