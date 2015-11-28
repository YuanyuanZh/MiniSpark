#!/usr/bin/env python
import logging


def create_debug_file(filename, level=logging.DEBUG):
    logging.basicConfig(filename=filename, level=level)


def debug_file(information, debug=True):
    if debug: logging.debug(information)


def debug_print(information, debug=True):
    if debug: print information
