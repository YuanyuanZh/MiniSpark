#!/usr/bin/env python


from src.util.util_pickle import *
import unittest
from src.rdd.rdd import *


class TestUtilPickle(unittest.TestCase):
    def test_pickle_rdd(self):
        """
        Test pickle and unpickle object funcitons.
        """
        file_path = '../files/wordcount'
        partitions = partition.FilePartition(file_path, 1).partition()

        lines = TextFile(file_path, partitions, 1)
        f = FlatMap(lines, lambda x: x.split(' '))
        m = Map(f, lambda x: (x, 1))

        new_rdd = unpickle_object(pickle_object(m))
        self.assertTrue(isinstance(new_rdd, Map))


if __name__ == '__main__':
    unittest.main()
