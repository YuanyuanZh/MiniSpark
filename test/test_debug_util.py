#!/usr/bin/env python

from src.util.util_debug import *


if __name__ == '__main__':
    # Test for creating debug log file
    create_debug_file('/tmp/test_log.log')

    # Test for writing debug information into log file
    debug_file('test_file_01')
    debug_file('test_file_02')
    debug_file('test_file_03')

    # Test for printing debug information on screen
    debug_print('test_print_01')
    debug_print('test_print_02')
    debug_print('test_print_03')