#!/usr/bin/env python
import zerorpc
from util_debug import *


def get_client(ip):
    c = zerorpc.Client()
    c.connect("tcp://{0}".format(ip))
    return c


def execute_command(client, func, *args):
    try:
        return func(*args)
    except zerorpc.LostRemote:
        debug_print("Lost Remote")
        return None
    finally:
        client.close()


def start_server(address, object):
    server = zerorpc.Server(object)
    server.bind("tcp://{0}".format(address))
    server.run()