# -*- coding: utf-8 -*-
from unittest import TestCase
from redisugar import RediSugar, sorted_set


class TestSorted_set(TestCase):
    redisugar = None

    @classmethod
    def setUpClass(cls):
        cls.redisugar = RediSugar.get_sugar(db=1)

    @classmethod
    def tearDownClass(cls):
        keys = [key for key in list(cls.redisugar.redis.scan_iter()) if key.startswith('zset_')]
        if keys:
            cls.redisugar.redis.delete(*keys)

    # def test_intersection_store(self):
    #     self.fail()
    #
    # def test_union_store(self):
    #     self.fail()
    #
    # def test__make_writable(self):
    #     self.fail()
    #
    # def test_copy(self):
    #     self.fail()
    #
    # def test_clear(self):
    #     self.fail()
    #
    # def test_discard(self):
    #     self.fail()
    #
    # def test_remove(self):
    #     self.fail()
    #
    # def test_add(self):
    #     self.fail()
    #
    # def test_score(self):
    #     self.fail()
    #
    # def test_incr_by(self):
    #     self.fail()
    #
    # def test_rank(self):
    #     self.fail()
    #
    # def test_count(self):
    #     self.fail()
    #
    # def test_range(self):
    #     self.fail()
    #
    # def test_range_by_score(self):
    #     self.fail()
    #
    # def test_remove_range(self):
    #     self.fail()
    #
    # def test_remove_range_by_score(self):
    #     self.fail()
