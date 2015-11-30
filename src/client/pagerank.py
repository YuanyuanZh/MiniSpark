from src.rdd import rdd
from src.rdd import partition
import sys
import re


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    rst = []
    for url in urls:
        rst.append((url, rank / num_urls))
    return rst


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

if __name__ == '__main__':
    filename = sys.argv[1]
    #
    # partitions = partition.RangePartition(filename,1).partition()
    # for p in partitions:
    t = rdd.TextFile(filename)
    m = rdd.Map(t,(lambda urls: parseNeighbors(urls)))
    links = rdd.GroupByKey(m)
    ranks = rdd.Map(links,lambda url_neighbors: (url_neighbors[0], 1.0))
    for iteration in range(int(sys.argv[2])):
        joins = rdd.Join([links,ranks])
        contribs = rdd.FlatMap(joins,lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
        rbk = rdd.ReduceByKey(contribs,lambda a,b:a+b)
        ranks = rdd.MapValue(rbk,lambda rank: rank * 0.85 + 0.15)
    #     collect = ranks.collect()
    # print(collect)
    print(ranks.get_lineage())
    print(t.partitions())
    print(m.partitions())
    print(links.partitions())
    print(ranks.partitions())
    print(joins.partitions())
    print(contribs.partitions())
    print(rbk.partitions())
    print(ranks.partitions())

