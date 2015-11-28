#!/usr/bin/env python


from src.util.util_pickle import *
import unittest
from src.rdd.rdd import *


class TestUtilPickle(unittest.TestCase):
    def test_pickle_rdd(self):
        """
        Test pickle and unpickle object funcitons.
        """
        lines = TextFile('../files/wordcount',partitions.get(p), 1)
        f = FlatMap(lines, lambda x: x.split(' '))
        m = Map(f, lambda x: (x, 1))
        self.assertEqual(m, unpickle_object(pickle_object(m)))


if __name__ == '__main__':
    unittest.main()
