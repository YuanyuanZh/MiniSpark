#!/usr/bin/env python

from cloudpickle import *


def pickle_object(input):
    """
    Convert the input object to a pickle object string.

    :param input: input object
    :return: pickle string.
    """
    output = StringIO.StringIO()
    pickler = CloudPickler(output)
    pickler.dump(input)
    return output.getvalue()


def unpickle_object(objstr):
    """
    Convert the pickle object string to an object.

    :param input: input object string
    :return: object.
    """
    input = StringIO.StringIO(objstr)
    unpickler = pickle.Unpickler(input)
    return unpickler.load()