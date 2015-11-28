from src.rdd import rdd
from src.rdd import partition
import sys

if __name__ == '__main__':
    filename = sys.argv[1]

    def compute_contribs(l, r):
        return l/len(r)

    partitions = partition.FilePartition(filename,1).partition()
    for p in partitions:
        t = rdd.TextFile(filename,partitions.get(p))
        links = rdd.Map(t,lambda s: s.split())
        ranks = rdd.Map(links,lambda url_neighbors: (url_neighbors[0], 1.0))
        for iteration in range(int(sys.argv[2])):
            joins = rdd.Join(links, ranks)
            contribs = rdd.FlatMap(joins,lambda url_urls_rank: compute_contribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            ranks = rdd.ReduceByKey(contribs,lambda a,b:a+b)
            rst = rdd.MapValue(ranks,lambda rank: rank * 0.85 + 0.15)
            collect = rst.collect()
            print(collect)