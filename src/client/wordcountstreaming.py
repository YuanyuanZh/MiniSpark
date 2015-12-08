import random
import gevent
from src.client.basicclient import StreamingClient

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



def send_data(client_address, data):
    client_s = get_client(client_address, 1)
    execute_command(client_s, client_s.get_streaming_message, data)


def get_worker_data(job_id, value):
    format_string = '{job_id},{value}'
    return format_string.format(job_id=job_id, value=value)


#def get_master_data(job_id, worker_id, partition_id):
#    format_string = '{job_id},{worker_id},{partition_id}'
#    return format_string.format(job_id=job_id, worker_id=worker_id,
#                                partition_id=partition_id)


def send_word(job_id, master_addr):

    while True:
        master = get_client(master_addr)
        worker_list = execute_command(master, master.get_worker_list)
        #debug_print_by_name('wentao', str(worker_list))

        for worker_id, worker in worker_list.items():
            #debug_print_by_name('wentao', str(worker))
            value = random.randint(1, 10)
            worker_data = get_worker_data(job_id, value)
            send_data(worker['address'], worker_data)
#            master_data = get_master_data(job_id, worker_iterator, partition_iterator)
#            send_data(master_addr, master_data)
#            worker_iterator = increase_number(worker_iterator, worker_count)

        gevent.sleep(2)


class StreamingWordCountClient(StreamingClient):
    def __init__(self, filename, interval):
        self.filename = filename
        self.interval = interval

    def run(self, driver):
        RDD._config = {'num_partition_RBK': 2,
                       'num_partition_GBK': 2,
                       'split_size': 128,
                       "driver_addr": ""}
        RDD._streaming = 20
        lines = rdd.Streaming(driver.num_partition)
        f = rdd.FlatMap(lines, lambda x: parse_lines(x))
        m = rdd.Map(f, lambda x: (x, 1))
        counts = rdd.ReduceByKey(m, lambda a, b: a + b)
        counts.collect(driver)


if __name__ == '__main__':

    name, master_address, self_address, interval = sys.argv
    # word count streaming client
    word_count_client = StreamingWordCountClient(master_address, int(interval))
    obj = pickle_object(word_count_client)

    # assign job
    client = get_client(master_address)
    job_id = execute_command(client, client.get_job, obj, self_address)
    debug_print_by_name('wentao', str(job_id))

    # send data
    send_data_thread = gevent.spawn(send_word, job_id, master_address)
    print "[Client]Job Submited...."
    client_thread = gevent.spawn(word_count_client.start_server, self_address)
    gevent.joinall([send_data_thread, client_thread])

    # word_count_client.start_server(self_address)

