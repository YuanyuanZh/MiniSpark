from src.client.basicclient import BasicClient
from src.rdd import rdd
import sys
import re
from src.rdd.rdd import RDD
from src.util.util_zerorpc import *


def parse_lines(line):
    line = re.sub(r'\n', "", line)
    return line.split(' ')


def increase_number(value, mod):
    return (value + 1) % mod


def send_word(worker_list):
    worker_count = len(worker_list)
    partition_count = 20
    worker_iterator = 0
    partition_iterator = 0
    while True:
        partition_iterator = increase_number(partition_iterator, partition_count)
        worker_count = increase_number(worker_iterator, worker_count)
        client1 = get_client(worker_list[worker_iterator]['worker_id'])
        execute_command(client1, client1.send_word, partition_iterator, )

        partition_iterator = increase_number(partition_iterator, partition_count)
        worker_count = increase_number(worker_iterator, worker_count)
        client2 = get_client(worker_list[worker_iterator]['worker_id'])
        execute_command(client2, client2.send_word, partition_iterator)


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
    RDD._streaming = 20
    word_count_client = WordCountClient(sys.argv[1])
    word_count_client.run()
    word_count_client.start_server("0.0.0.0")
