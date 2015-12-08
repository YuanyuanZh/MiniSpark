#!/usr/bin/env python

import argparse
from src.client.pagerank import PageRankClient
from src.client.wordcount import WordCountClient
from src.master import Master
from src.worker import Worker
from src.util.util_pickle import *
from src.util.util_zerorpc import *

def init_wordcount_streaming_client_parser(subparsers):
    wordcount_parser = subparsers.add_parser('wordcount_streaming',
                              help='word count streaming client')
    wordcount_parser.add_argument('master_address',
                                         help='master address')
    wordcount_parser.add_argument('self_address', help='self address')
    wordcount_parser.set_defaults(action='wordcount_streaming')



def init_pagerank_client_parser(subparsers):
    pagerank_parser = subparsers.add_parser('pagerank',
                                                   help='pagerank client')
    pagerank_parser.add_argument('file_path', help='file path')
    pagerank_parser.add_argument('iterative', help='iterative times')
    pagerank_parser.add_argument('master_address',
                                        help='master address')
    pagerank_parser.add_argument('self_address', help='self address')
    pagerank_parser.set_defaults(action='page_rank')


def init_wordcount_client_parser(subparsers):
    wordcount_parser = subparsers.add_parser('wordcount',
                                             help='wordcount client')
    wordcount_parser.add_argument('file_path', help='file path')
    wordcount_parser.add_argument('master_address', help='master address')
    wordcount_parser.add_argument('self_address', help='self address')
    wordcount_parser.set_defaults(action='word_count')



def init_master_parser(subparsers):
    master_parser = subparsers.add_parser('master', help='master tool')
    master_parser.add_argument('port', help='master port')
    master_parser.add_argument('-d', '--debug',action="store_true",
                               help='debug information')
    master_parser.set_defaults(action='master')



def init_worker_parser(subparsers):
    worker_parser = subparsers.add_parser('worker', help='worker tool')
    worker_parser.add_argument('master_address', help='master address')
    worker_parser.add_argument('self_address', help='self address')
    worker_parser.add_argument('-d', '--debug', action="store_true",
                               help='debug information')
    worker_parser.set_defaults(action='worker')




if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Client tool for DoEnjoy mini-Spark')
    subparsers = PARSER.add_subparsers()
    init_wordcount_client_parser(subparsers)
    init_pagerank_client_parser(subparsers)
    init_wordcount_streaming_client_parser(subparsers)
    init_master_parser(subparsers)
    init_worker_parser(subparsers)

    ARGS = PARSER.parse_args()
    if ARGS.action == 'master':
        master = Master(ARGS.port, ARGS.debug)
        master.run()
    elif ARGS.action == 'worker':
        worker = Worker(ARGS.master_address, ARGS.self_address, ARGS.debug)
        worker.run()
    elif ARGS.action == 'page_rank':
        page_rank_client = PageRankClient(ARGS.file_path, ARGS.iterative)
        client = get_client(ARGS.master_address)
        execute_command(client,
                        client.get_job,
                        pickle_object(page_rank_client),
                        ARGS.self_address)
        page_rank_client.start_server("0.0.0.0")
    elif ARGS.action == 'word_count':
        word_count_client = WordCountClient(ARGS.file_path)
        client = get_client(ARGS.master_address)
        execute_command(client,
                        client.get_job,
                        pickle_object(word_count_client),
                        ARGS.self_address)
        word_count_client.start_server("0.0.0.0:" +
                                       ARGS.self_address.split(":")[1])
    elif ARGS.action == 'wordcount_streaming':
        wcsc = WordCountStreamingClient()
        client = get_client(ARGS.master_address)
        execute_command(client,
                        client.get_job,
                        pickle_object(wcsc),
                        ARGS.self_address)
        wcsc.start_server("0.0.0.0:" + ARGS.self_address.split(":")[1])