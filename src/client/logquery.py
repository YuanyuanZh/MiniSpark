from src.client.basicclient import BasicClient, MASTER_ADDRESS
from src.rdd import rdd
from src.rdd.rdd import RDD
from src.util.util_pickle import *
from src.util.util_zerorpc import execute_command
from src.util.util_zerorpc import get_client


def parse_warning(line):
    s = line.split(" ")
    rst = {s[1]: (s[2], s[6])}
    return rst


class LogQueryClient(BasicClient):
    def __init__(self, filename):
        self.filename = filename

    def run(self, driver):
        RDD._config = {'num_partition_RBK': 2,
                   'num_partition_GBK': 2,
                   'split_size': 128}
        lines = rdd.TextFile(self.filename)
        warnings = rdd.Filter(lines,lambda l: l.startswith("Warning"))
        worker0_warnings = rdd.Filter(warnings, lambda x: x.contains("worker0"))
        worker0_down_info = rdd.Map(worker0_warnings, lambda w: parse_warning(w))
        worker0_down_info.collect(driver)


if __name__ == '__main__':

    master_address = sys.argv[1]
    self_address = sys.argv[2]
    filepath = sys.argv[3]

    word_count_client = LogQueryClient("../../logquery.txt")
    new_rdd = unpickle_object(pickle_object(word_count_client))


    client = get_client(master_address)
    print "====="
    obj = pickle_object(word_count_client)
    print "====="
    execute_command(client, client.get_job, obj, self_address)
    print "====="
    word_count_client.start_server("0.0.0.0:" + self_address.split(":")[1])

