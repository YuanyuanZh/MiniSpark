import random
import gevent
from src.client.basicclient import BasicClient
from src.rdd import rdd
import sys
import re
from src.rdd.rdd import RDD
from src.util.util_pickle import pickle_object
from src.util.util_zerorpc import *


def parse_lines(line):
    line = re.sub(r'\n', "", line)
    return line.split(' ')


def increase_number(value, mod):
    return (value + 1) % mod


def send_word(job_id, master_addr):
    master = get_client(master_addr)
    partition_count = 20
    while True:
        worker_list = execute_command(master, master.get_worker_list)
        worker_ids=worker_list.keys()
        worker_count = len(worker_ids)
        worker_iterator = 0
        for partition_iterator in (0,partition_count):

            data = '{job_id},{partition_id},{value}'.format(job_id=job_id,
                                                            partition_id=partition_iterator,
                                                            value=random.randint(1,10)
                                                            )

            client1 = get_client(worker_list[worker_ids[worker_iterator]]['worker_id'])
            execute_command(client1, client1.get_streaming_message, data)
            # TODO Master.send_partition(partition_id, worker_id)
            #     Tell Master the partition and where it stored
            execute_command(master, master.send_partition, partition_iterator, worker_iterator)

            # Make a Replication
            worker_iterator = increase_number(worker_iterator, worker_count)
            client2 = get_client(worker_list[worker_ids[worker_iterator]]['worker_id'])
            execute_command(client2, client2.get_streaming_message, data)
            execute_command(master, master.send_partition, partition_iterator, worker_iterator)

        gevent.sleep(0.5)


class WordCountClient(BasicClient):
    def __init__(self, filename):
        self.filename = filename

    def run(self,driver):
        RDD._config = {'num_partition_RBK': 2,
                   'num_partition_GBK': 2,
                   'split_size': 128,
                   "driver_addr": ""}
        RDD._streaming = 20
        lines = rdd.TextFile(self.filename)
        f = rdd.FlatMap(lines, lambda x: parse_lines(x))
        m = rdd.Map(f, lambda x: (x, 1))
        counts = rdd.ReduceByKey(m, lambda a, b: a + b)
        counts.collect(driver)


if __name__ == '__main__':
    master_address=sys.argv[1]
    self_address=sys.argv[2]
    word_count_client = WordCountClient(sys.argv[1])

    client = get_client(master_address)
    obj = pickle_object(word_count_client)
    job_id = execute_command(client, client.get_job, obj, self_address)
    gevent.spawn(send_word, job_id, master_address)
    word_count_client.start_server(self_address)