from src.client.basicclient import BasicClient
from src.rdd import rdd
import sys
import re
from src.rdd.rdd import RDD


def parse_lines(line):
    line = re.sub(r'\n', "", line)
    return line.split(' ')


class WordCountClient(BasicClient):
    def __init__(self, filename):
        super(WordCountClient, self).__init__()
        self.filename = filename

    def run(self):
        lines = rdd.TextFile(self.filename)
        f = rdd.FlatMap(lines, lambda x: parse_lines(x))
        m = rdd.Map(f, lambda x: (x, 1))
        counts = rdd.ReduceByKey(m, lambda a, b: a + b)
        counts.collect()


if __name__ == '__main__':
    RDD._config = {'num_partition_RBK': 2,
                   'num_partition_GBK': 2,
                   'split_size': 128,
                   "driver_addr": ""}
    word_count_client = WordCountClient(sys.argv[1])
    word_count_client.run()
    word_count_client.start_server("0.0.0.0")
