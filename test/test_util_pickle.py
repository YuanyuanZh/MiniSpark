#!/usr/bin/env python


from src.util.util_pickle import *
from src.client.wordcount import WordCountClient
import unittest


class TestUtilPickle(unittest.TestCase):
    def test_pickle_rdd(self):
        """
        Test pickle and unpickle object functions.
        """

        word_count_client = WordCountClient("../../wordcount")
        new_rdd = unpickle_object(pickle_object(word_count_client))
        self.assertTrue(isinstance(new_rdd, WordCountClient))


if __name__ == '__main__':
    unittest.main()
