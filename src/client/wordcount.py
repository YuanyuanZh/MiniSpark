from src.rdd import rdd
from src.rdd import partition
import sys

if __name__ == '__main__':
    filename = sys.argv[1]
    partitions = partition.FilePartition(filename,1).partition()
    for p in partitions:
        lines = rdd.TextFile(filename,partitions.get(p), 1)
        f = rdd.FlatMap(lines,lambda x: x.split(' '))
        m = rdd.Map(f,lambda x: (x, 1))
        counts = rdd.ReduceByKey(m,lambda a, b: a+b)
        output = counts.collect()
        print(output)
        print (counts.get_lineage())
        print(lines.partitions())
        print (f.partitions())
        print(m.partitions())
        print(counts.partitions())