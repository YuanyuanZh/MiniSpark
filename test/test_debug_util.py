from src.util.util_debug import *


if __name__ == '__main__':
    create_debug_file('/tmp/test_log.log')
    debug_information('test_file_01')
    debug_information('test_file_02')
    debug_information('test_file_03')
    print_information('test_print_01')
    print_information('test_print_02')
    print_information('test_print_03')