from src.util.util_zerorpc import start_server


class BasicClient(object):
    def __init__(self):
        pass

    def run(self):
        pass

    @staticmethod
    def _print_message(message):
        print message

    def start_server(self, ip):
        start_server(ip, self)
