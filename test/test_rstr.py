# -*- coding: utf-8 -*-
from unittest import TestCase
from redisugar import RediSugar
from redisugar import rstr


class TestRstr(TestCase):
    redisugar = None

    @classmethod
    def setUpClass(cls):
        cls.redisugar = RediSugar.get_sugar(db=1)

    @classmethod
    def tearDownClass(cls):
        keys = [key for key in list(cls.redisugar.redis.scan_iter()) if key.startswith('set_')]
        if keys:
            cls.redisugar.redis.delete(*keys)

    def test_multi_set_get(self):
        d = {'test_1': 'v1', 'test_2': 'v2'}
        kwargs = {'test_3': 'v3', 'test_4': 'v4'}
        keys = d.keys() + kwargs.keys()
        rstr.multi_set(self.redisugar, d, **kwargs)
        for key in keys:
            self.assertIn(key, self.redisugar)
        pairs = zip(d.keys(), rstr.multi_get(self.redisugar, d.keys()))
        for k, v in pairs:
            self.assertEqual(d[k], v)
        pairs = zip(kwargs.keys(), rstr.multi_get(self.redisugar, [], *kwargs.keys()))
        for k, v in pairs:
            self.assertEqual(kwargs[k], v)
        self.redisugar.redis.delete(*keys)

    def test_multi_set_not_exist(self):
        self.redisugar['test_v1'] = 'v1'
        d = {'test_v1': 'v1', 'test_v2': 'v2'}
        self.assertRaises(ValueError, rstr.multi_set_not_exist, self.redisugar, d)
        self.assertNotIn('test_v2', self.redisugar)
        del self.redisugar['test_v1']

    def test__iadd(self):
        s = rstr(self.redisugar, 'test_iadd', 'abc')
        s += 'def'
        self.assertEqual('abcdef', str(s))
        del self.redisugar[s.key]

    def test__getitem(self):
        s = rstr(self.redisugar, 'test_getitem', 'abcdef')
        self.assertEqual('ab', s[:2])
        self.assertEqual('ace', s[::2])
        self.assertRaises(IndexError, s.__getitem__, 10)
        del self.redisugar[s.key]

    def test_set(self):
        s = rstr(self.redisugar, 'test_set', 'abc')
        self.assertEqual('abc', str(s))
        s.set('123')
        self.assertEqual('123', str(s))
        del self.redisugar[s.key]

    def test_decrease(self):
        s = rstr(self.redisugar, 'test_decrease', '10')
        s.decrease()
        self.assertEqual('9', str(s))
        s.decrease(4)
        self.assertEqual('5', str(s))
        s.decrease(10)
        self.assertEqual('-5', str(s))
        s.decrease(-5)
        self.assertEqual('0', str(s))
        del self.redisugar[s.key]

    def test_increase(self):
        s = rstr(self.redisugar, 'test_increase', '1')
        s.increase()
        self.assertEqual('2', str(s))
        s.increase(0.5)
        self.assertEqual('2.5', str(s))
        s.increase(-2.5)
        self.assertEqual('0', str(s))
        del self.redisugar[s.key]

    def test_set_range(self):
        s = rstr(self.redisugar, 'test_set_range', 'abc')
        s.set_range(1, 'aa')
        self.assertEqual('aaa', str(s))
        s.set_range(1, 'bbb')
        self.assertEqual('abbb', str(s))
        s.set_range(2, '')
        self.assertEqual('abbb', str(s))
        del self.redisugar[s.key]
