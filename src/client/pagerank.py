from src.client.basicclient import BasicClient, MASTER_ADDRESS
from src.rdd import rdd
import sys
import re
from src.rdd.rdd import RDD
from src.util.util_pickle import pickle_object
from src.util.util_zerorpc import execute_command
from src.util.util_zerorpc import get_client


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    rst = []
    for url in urls:
        rst.append((url, rank / num_urls))
    return rst


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


class PageRankClient(BasicClient):
    def __init__(self, filename):
        self.filename = filename

    def run(self,driver):
        RDD._config = {'num_partition_RBK': 2,
                   'num_partition_GBK': 2,
                   'split_size': 128,
                   }
        t = rdd.TextFile(self.filename)
        m = rdd.Map(t, (lambda urls: parseNeighbors(urls)))
        links = rdd.GroupByKey(m)
        ranks = rdd.Map(links, lambda url_neighbors: (url_neighbors[0], 1.0))
        for iteration in range(5):
            joins = rdd.Join([links, ranks])
            contribs = rdd.FlatMap(joins,
                                   lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            rbk = rdd.ReduceByKey(contribs, lambda a, b: a + b)
            ranks = rdd.MapValue(rbk, lambda rank: rank * 0.85 + 0.15)
        ranks.collect(driver)


if __name__ == '__main__':

    master_address = sys.argv[1]
    self_address = sys.argv[2]
    filepath = sys.argv[3]

    page_rank_client = PageRankClient(filepath)
    # page_rank_client = PageRankClient(sys.argv[1])
    client = get_client(master_address)
    execute_command(client, client.get_job, pickle_object(page_rank_client), self_address)
    print "[Client]Job Submited...."
    page_rank_client.start_server(self_address)

