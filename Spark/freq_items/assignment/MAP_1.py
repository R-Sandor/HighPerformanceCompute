import sys


def createC1(dataset):
    "Create a list of candidate item sets of size one."
    c1 = []
    for transaction in dataset:
        for item in transaction:
            if [item] not in c1:
                c1.append([item])
    c1.sort()
    # frozenset because it will be a ket of a dictionary.
    return list(map(frozenset, c1))


def scanD(dataset, candidates, min_support):
    "Returns all candidates that meets a minimum support level"
    sscnt = {}
    for tid in dataset:
        for can in candidates:
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
    return retlist, support_data


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


#  coppied from https://gist.github.com/marcelcaraciolo/1423287
def apriori(dataset, minsupport=0.5):
    "Generate a list of candidate item sets"
    C1 = createC1(dataset)
    D = map(set, dataset)
    L1, support_data = scanD(D, C1, minsupport)
    L = [L1]
    k = 2
    while len(L[k - 2]) > 0:
        Ck = aprioriGen(L[k - 2], k)
        Lk, supK = scanD(D, Ck, minsupport)
        support_data.update(supK)
        L.append(Lk)
        k += 1


# Main execution:
# inspired by https://gist.github.com/marcelcaraciolo/1423287
datasets = []
minimum_support = 0.3
for line in sys.stdin:
    line = line.strip()
    items = line.split()
    datasets.append(items)
ds_result = apriori(datasets, minimum_support)

for data in ds_result:
    x, y = data
    x = int(x)
    y = int(y)
    print("%d %d" % (x, y))