from src.client.basicclient import BasicClient
from src.rdd import rdd
import sys
import re


def parse_lines(line):
    line = re.sub(r'\n', "", line)
    return line.split(' ')


class WordCountClient(BasicClient):
    def __init__(self, filename):
        self.filename = filename

    def run(self):
        lines = rdd.TextFile(self.filename)
        f = rdd.FlatMap(lines,lambda x: parse_lines(x))
        m = rdd.Map(f,lambda x: (x, 1))
        counts = rdd.ReduceByKey(m,lambda a, b: a+b)
        counts.collect()


if __name__ == '__main__':
    word_count_client = WordCountClient(sys.argv[1])
    word_count_client.run()
    word_count_client.start_server("0.0.0.0")