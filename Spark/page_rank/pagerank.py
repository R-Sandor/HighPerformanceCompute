import re
import sys
from operator import add
from pyspark import SparkContext


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


sc = SparkContext(appName="PythonPageRank")


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r"\s+", urls)
    return parts[0], parts[1]


if len(sys.argv) != 4:
    print(
        "Usage: pagerank.py <input_file> <output_file> <number of iterations>",
        file=sys.stderr,
    )
    exit(-1)
lines = sc.textFile(sys.argv[1], 1)
print("outputs for inital run for understanding")
links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
for link, neighs in links.collect():
    (print("\n%s has neigbors (directed): " % link),)
    for neigh in neighs:
        (print(" %s " % neigh),)
ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
print("\n\n***setting up initial rank to 1.0***")
for page, rank in ranks.collect():
    print("\ns has initial rank: %s" % (page, rank))
temp_join = links.join(ranks)
contribs = temp_join.flatMap(
    lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1])
)
print(
    "\n***calculating contirbutions for each page from nebhiours by rank/number of links going out of the page\n***"
)
for page, contrib in contribs.collect():
    print("page %s is getting contribution of %s from its neighbor" % (page, contrib))
temp_aggreg = contribs.reduceByKey(add)
print("\n*** adding all the contirbutions****\n")
for page, aggreg in temp_aggreg.collect():
    print(
        "page %s has aggregated contribution of %s from all its neighbor"
        % (page, aggreg)
    )
print(
    "\n*** recalculating ranks using damping factor and guessing iniatial PR as 0****\n"
)
temp_ranks = temp_aggreg.mapValues(lambda rank: rank * 0.85 + 0.15)
for page, rank in temp_ranks.collect():
    print("page %s has an updated rank of %s" % (page, rank))
print("\n*** doing the same steps for given number of iterations ***\n")
for iteration in range(int(sys.argv[3])):
    print("\n*** Iteration %d***" % iteration)
    contribs = links.join(ranks).flatMap(
        lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1])
    )
    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
    for link, rank in ranks.collect():
        print("%s has rank: %s." % (link, rank))
ranks.repartition(1).saveAsTextFile(sys.argv[2])
sc.stop()
