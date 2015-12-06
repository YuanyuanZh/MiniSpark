#!/usr/bin/env python
import logging

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


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

def debug_print_by_name(name, message, debug=True):
    if name == 'wentao':
        if debug:
            print bcolors.FAIL + 'Wentao Wentao Wentao Wentao Wentao Wentao Wentao' + bcolors.ENDC
        debug_print(bcolors.FAIL + message + bcolors.ENDC)
