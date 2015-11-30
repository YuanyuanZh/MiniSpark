from src.client.basicclient import BasicClient
from src.rdd import rdd
import sys
import re
from src.rdd.rdd import RDD


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
        super(PageRankClient, self).__init__()
        self.filename = filename

    def run(self):
        t = rdd.TextFile(self.filename)
        m = rdd.Map(t, (lambda urls: parseNeighbors(urls)))
        links = rdd.GroupByKey(m)
        ranks = rdd.Map(links, lambda url_neighbors: (url_neighbors[0], 1.0))
        for iteration in range(int(sys.argv[2])):
            joins = rdd.Join([links, ranks])
            contribs = rdd.FlatMap(joins,
                                   lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            rbk = rdd.ReduceByKey(contribs, lambda a, b: a + b)
            ranks = rdd.MapValue(rbk, lambda rank: rank * 0.85 + 0.15)
        ranks.collect()


if __name__ == '__main__':
    RDD._config = {'num_partition_RBK': 2,
                   'num_partition_GBK': 2,
                   'split_size': 128,
                   "driver_addr": "",
                   "master_addr": ""}
    page_rank_client = PageRankClient(sys.argv[1])
    page_rank_client.run()
    page_rank_client.start_server("0.0.0.0")
