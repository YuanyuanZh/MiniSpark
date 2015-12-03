from src.util.util_zerorpc import start_server

MASTER_ADDRESS = '0.0.0.0:3000'


class BasicClient(object):
    def __init__(self):
        pass

    def run(self,driver):
        pass

    @staticmethod
    def _print_message(message):
        print message

    def start_server(self, address):
        start_server(address, self)
