import random
import gevent
from src.client.basicclient import StreamingClient
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
    worker_data_format = '{job_id},{partition_id},{value}'
    master_data_format = '{job_id},{worker_id},{partition_id}'
    master = get_client(master_addr)
    partition_count = 20
    while True:
        worker_list = execute_command(master, master.get_worker_list)
        debug_print_by_name('wentao', str(worker_list))

    while True:
        worker_list = execute_command(master, master.get_worker_list)
        worker_ids = worker_list.keys()
        worker_count = len(worker_ids)
        worker_iterator = 0
        debug_print_by_name('wentao', str(worker_ids))

        if len(worker_ids) > 0:
            debug_print_by_name('wentao', 'Enter')

            for partition_iterator in (0, partition_count):

                worker_data = worker_data_format.format(job_id=job_id, partition_id=partition_iterator,
                                                    value=random.randint(1,10))

                client1 = get_client(worker_list[worker_ids[worker_iterator]]['worker_id'])
                execute_command(client1, client1.get_streaming_message, worker_data)
                # TODO Master.send_partition(partition_id, worker_id)
                #     Tell Master the partition and where it stored
                master_data = master_data_format.format(job_id=job_id, worker_id=worker_iterator,
                                                        partition_id=partition_iterator)
                execute_command(master, master.send_partition, master_data)

                # Make a Replication
                worker_iterator = increase_number(worker_iterator, worker_count)
                if len(worker_ids) != 1:
                    client2 = get_client(worker_list[worker_ids[worker_iterator]]['worker_id'])
                    execute_command(client2, client2.get_streaming_message, worker_data)
                    master_data = master_data_format.format(job_id=job_id, worker_id=worker_iterator,
                                                        partition_id=partition_iterator)
                    execute_command(master, master.send_partition, master_data)

        #gevent.sleep(0.5)


class StreamingWordCountClient(StreamingClient):
    def __init__(self, filename):
        self.filename = filename

    def run(self, driver):
        RDD._config = {'num_partition_RBK': 2,
                   'num_partition_GBK': 2,
                   'split_size': 128,
                   "driver_addr": ""}
        RDD._streaming = 20
        lines = rdd.TextFile(self.filename)
        f = rdd.FlatMap(lines, lambda x: parse_lines(x))
        m = rdd.Map(f, lambda x: (x, 1))
        counts = rdd.ReduceByKey(m, lambda a, b: a + b)
        #counts.collect(driver)


if __name__ == '__main__':
    name, master_address, self_address = sys.argv
    # word count streaming client
    word_count_client = StreamingWordCountClient(master_address)
    obj = pickle_object(word_count_client)

    # assign job
    client = get_client(master_address)
    job_id = execute_command(client, client.get_job, obj, self_address)

    # send data
    gevent.spawn(send_word, job_id, master_address)
    print "[Client]Job Submited...."
    word_count_client.start_server(self_address)
    #word_count_client.start_server(self_address)