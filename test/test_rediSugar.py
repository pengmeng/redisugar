# -*- coding: utf-8 -*-
from unittest import TestCase
from redisugar import RediSugar, rlist, rdict, rset


class TestRediSugar(TestCase):
    redisugar = None

    @classmethod
    def setUpClass(cls):
        cls.redisugar = RediSugar.get_sugar(db=1)
        keys = [key for key in cls.redisugar.keys() if key.startswith('test_')]
        if keys:
            cls.redisugar.redis.delete(*keys)

    def test__set_get_item(self):
        self.redisugar['test_list'] = [1, 2, 3]
        self.assertIsInstance(self.redisugar['test_list'], rlist)
        self.assertEqual(3, len(self.redisugar['test_list']))
        self.redisugar['test_dict'] = dict.fromkeys([1, 2, 3], 0)
        self.assertIsInstance(self.redisugar['test_dict'], rdict)
        self.assertEqual(3, len(self.redisugar['test_dict']))
        self.assertDictEqual({'1': '0', '3': '0', '2': '0'}, self.redisugar['test_dict'].copy())
        self.redisugar['test_set'] = {1, 2, 3}
        self.assertIsInstance(self.redisugar['test_set'], rset)
        self.assertEqual(3, len(self.redisugar['test_set']))
        self.assertSetEqual({'1', '2', '3'}, self.redisugar['test_set'])

    def test_get(self):
        self.redisugar['1'] = '1'
        self.assertEqual('1', self.redisugar.get('1'))
        self.assertIsNone(self.redisugar.get('2'))
        del self.redisugar['1']

    def test_pop(self):
        self.redisugar['1'] = '1'
        self.assertEqual('1', self.redisugar.pop('1'))
        self.assertEqual('2', self.redisugar.pop('2', '2'))
        self.assertRaises(KeyError, self.redisugar.pop, '2')

    def test_setdefault(self):
        self.redisugar['1'] = '1'
        self.assertEqual('1', self.redisugar.setdefault('1', '2'))
        self.assertEqual('2', self.redisugar.setdefault('2', '2'))
        del self.redisugar['1']
        del self.redisugar['2']

    def test_getset(self):
        self.redisugar['1'] = '1'
        self.assertEqual('1', self.redisugar.getset('1', '2'))
        self.assertEqual('2', self.redisugar['1'])
        del self.redisugar['1']