#!/usr/bin/env python

import argparse
from src.client.wordcount import WordCountClient
from src.util.util_pickle import *



def init_client_parse(subparsers):
    client_parser = subparsers.add_parser('client', help='client tool')
    client_parser.add_argument('parameters',nargs='+', help='bar help')


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Client tool for DoEnjoy mini-Spark')
    subparsers = PARSER.add_subparsers(help='action parser')

    # parser_a = subparsers.add_parser('a', help='a help')
    #
    # PARSER.add_argument('action', help='record, publish')
    # PARSER.add_argument('-f', '--file',
    #                     help='configuration file for the replay demo')
    # ARGS = PARSER.parse_args()
    #
    # if ARGS.action == 'record':
    #     record_data(ARGS.file)
    # elif ARGS.action == 'publish':
    #     publish_tsdata(ARGS.file)

    word_count_client = WordCountClient("../../wordcount")
    new_rdd = unpickle_object(pickle_object(word_count_client))
