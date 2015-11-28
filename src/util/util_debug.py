#!/usr/bin/env python
import logging


def create_debug_file(filename, level=logging.DEBUG):
    """
    Function to create a debug file.

    :param filename: File name of new log file
    :param level: log level and default is logging.DEBUG
    """
    logging.basicConfig(filename=filename, level=level)


def debug_file(message, debug=True):
    """
    If debug is True, write debug message into log file.

    :param information: debug message
    :param debug: debug status
    """
    if debug: logging.debug(message)


def debug_print(message, debug=True):
    """
    If debug is True, print debug message on screen.

    :param information: debug message
    :param debug: debug status
    """
    if debug: print message
