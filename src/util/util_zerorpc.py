#!/usr/bin/env python
import zerorpc


def get_client(ip):
    c = zerorpc.Client()
    c.connect("tcp://{0}".format(ip))
    return c


def execute_command(client, func, *args):
    try:
        return func(*args)
    except zerorpc.LostRemote:
        return None
    finally:
        client.close()