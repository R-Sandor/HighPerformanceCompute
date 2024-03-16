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

# Main apriori task calls aprioriGen
def apriori(dataset, support=0.5):
    # Scan the dataset for canidates
    C1 = createC1(dataset)
    # Create list
    D = list(map(set,dataset))
    K=2
    for i in range(K):
    	L1,supp = scanD(D ,C1, support)
    	C1 = aprioriGen(L1,i+1)
    return L1

# Main execution:
# inspired by https://gist.github.com/marcelcaraciolo/1423287
dataset=[]
support = 0.3
for line in sys.stdin:
    item = line.strip()
    line_items = line.split()
    dataset.append(line_items)

L1 = apriori(dataset, support)

out=[]
for i in L1:
    x,y = i
    x = int(x)
    y = int(y)
    print("{:n} {:n}".format(x,y))
