from src.client.basicclient import BasicClient, MASTER_ADDRESS
from src.rdd import rdd
import sys
import re
from src.rdd.rdd import RDD
from src.util.util_pickle import *
from src.util.util_zerorpc import execute_command
from src.util.util_zerorpc import get_client


def parse_lines(line):
    line = re.sub(r'\n', "", line)
    return line.split(' ')


class WordCountClient(BasicClient):
    def __init__(self, filename):
        self.filename = filename

    def run(self, driver):
        RDD._config = {'num_partition_RBK': 2,
                   'num_partition_GBK': 2,
                   'split_size': 128}
        lines = rdd.TextFile(self.filename)
        f = rdd.FlatMap(lines, lambda x: parse_lines(x))
        m = rdd.Map(f, lambda x: (x, 1))
        counts = rdd.ReduceByKey(m, lambda a, b: a + b)
        counts.collect(driver)



if __name__ == '__main__':
    master_address = sys.argv[1]
    self_address = sys.argv[2]
    filepath = sys.argv[3]

    #word_count_client = WordCountClient("/Users/kaijiezhou/Documents/workspaces/Python/DoEnjoy-project3/files/wordcount")
    word_count_client = WordCountClient(filepath)

    new_rdd = unpickle_object(pickle_object(word_count_client))


    client = get_client(master_address)
    obj = pickle_object(word_count_client)
    execute_command(client, client.get_job, obj, self_address)
    print "[Client]Job Submited...."
    word_count_client.start_server(self_address)
