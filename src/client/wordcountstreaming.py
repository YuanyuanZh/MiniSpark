import gevent
from src.client.basicclient import BasicClient, MASTER_ADDRESS
from src.rdd import rdd
import sys
import re
from src.rdd.rdd import RDD
from src.util.util_pickle import pickle_object
from src.util.util_zerorpc import *
import random

def parse_lines(line):
    line = re.sub(r'\n', "", line)
    return line.split(' ')


def increase_number(value, mod):
    return (value + 1) % mod


def send_word(worker_list, job_id):
    worker_count = len(worker_list)
    partition_count = 20

    while True:
        worker_iterator = 0
        for partition_iterator in (0, partition_count):
            value = random.randint(0,10)
            client1 = get_client(worker_list[worker_iterator]['address'])
            execute_command(client1, client1.get_streaming_message, '{job_id},{partition_id},{value}'.format(
                job_id=job_id,
                partition_id=partition_iterator,
                value=value
            ))

            worker_iterator = increase_number(worker_iterator, worker_count)
            client2 = get_client(worker_list[worker_iterator]['address'])
            execute_command(client2, client2.get_streaming_message, '{job_id},{partition_id},{value}'.format(
                job_id=job_id,
                partition_id=partition_iterator,
                value=value
            ))
        gevent.sleep(0.5)


class WordCountClientStreaming(BasicClient):
    def __init__(self):
        super(WordCountClientStreaming, self).__init__()
        self.interval = 20

    def run(self):
        data = rdd.Streaming()
        f = rdd.FlatMap(data, lambda x: parse_lines(x))
        m = rdd.Map(f, lambda x: (x, 1))
        counts = rdd.ReduceByKey(m, lambda a, b: a + b)
        counts.collect()


if __name__ == '__main__':
    RDD._config = {'num_partition_RBK': 2,
                   'num_partition_GBK': 2,
                   'split_size': 128,
                   "driver_addr": ""}
    word_count_client = WordCountClientStreaming()
    client = get_client(MASTER_ADDRESS)
    job_id = execute_command(client, client.get_job, pickle_object(word_count_client), sys.argv[2])

    client = get_client(MASTER_ADDRESS)
    worker_list = execute_command(client, client.get_worker_list)

    gevent.spawn(send_word, worker_list, job_id)

    word_count_client.run()
    word_count_client.start_server("0.0.0.0")
