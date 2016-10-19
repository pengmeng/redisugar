# -*- coding: utf-8 -*-
from unittest import TestCase

from redisugar import RediSugar
from redisugar import rdict


class TestRdict(TestCase):
    redisugar = None

    @classmethod
    def setUpClass(cls):
        cls.redisugar = RediSugar.getSugar(db=1)

    def test__init(self):
        self.assertRaises(TypeError, rdict, self.redisugar, 'dict_dummy', [], [])
        self.assertRaises(TypeError, rdict, self.redisugar, 'dict_dummy', [1, 2])
        self.assertRaises(TypeError, rdict, self.redisugar, 'dict_dummy', [('1', 2), 3])
        self.assertRaises(TypeError, rdict, self.redisugar, 'dict_dummy', 3)
        d = rdict(self.redisugar, 'dict_len', [('a', 1), ('b', 2)], c=3, d=4)
        self.assertEqual(4, len(d))
        d.clear()
        d = rdict(self.redisugar, 'dict_init', {'a': '1', 'b': '2'})
        self.assertDictEqual({'a': '1', 'b': '2'}, d.copy())
        d.clear()

    def test_clear(self):
        d = rdict(self.redisugar, 'dict_clear')
        d.clear()
        d.update([('a', 1), ('b', 2)], c=3, d=4)
        self.assertEqual(4, len(d))
        d.clear()
        self.assertEqual(0, len(d))

    def test_fromkeys(self):
        l = [str(x) for x in range(5)]
        d = rdict.fromkeys(self.redisugar, 'dict_fromkeys', l, 'a')
        _d = dict.fromkeys(l, 'a')
        self.assertDictEqual(d.copy(), _d)
        d.clear()
        d = rdict.fromkeys(self.redisugar, 'dict_fromkeys', l)
        _d = dict.fromkeys(l, 'None')
        self.assertDictEqual(d.copy(), _d)
        d.clear()

    def test_get(self):
        d = rdict(self.redisugar, 'dict_get')
        d.clear()
        d.update([('a', 1), ('b', 2)], c=3, d=4)
        self.assertEqual('1', d.get('a'))
        self.assertEqual(None, d.get('z'))
        self.assertEqual(2, d.get('z', 2))
        d.clear()

    def test_keys(self):
        d = rdict(self.redisugar, 'dict_keys')
        d.clear()
        d.update([('a', 1), ('b', 2)], c=3, d=4)
        self.assertItemsEqual(['a', 'b', 'c', 'd'], d.keys())
        d.clear()

    def test_values(self):
        d = rdict(self.redisugar, 'dict_values')
        d.clear()
        d.update([('a', 1), ('b', 2)], c=3, d=4)
        self.assertItemsEqual(['1', '2', '3', '4'], d.values())
        d.clear()

    def test_iterkeys(self):
        d = rdict(self.redisugar, 'dict_iterkeys')
        d.clear()
        d.update([('a', 1), ('b', 2)], c=3, d=4)
        self.assertItemsEqual(['a', 'b', 'c', 'd'], list(d.iterkeys()))
        d.clear()

    def test_itervalues(self):
        d = rdict(self.redisugar, 'dict_itervalues')
        d.clear()
        d.update([('a', 1), ('b', 2)], c=3, d=4)
        self.assertItemsEqual(['1', '2', '3', '4'], list(d.itervalues()))
        d.clear()

    def test_iteritems(self):
        d = rdict(self.redisugar, 'dict_iteritems')
        d.clear()
        d.update([('a', 1), ('b', 2)], c=3, d=4)
        _d = dict(d.iteritems())
        self.assertDictEqual(_d, d.copy())
        d.clear()

    def test_pop(self):
        d = rdict(self.redisugar, 'dict_pop')
        d.clear()
        d.update([('a', 1), ('b', 2)], c=3, d=4)
        self.assertEqual('1', d.pop('a'))
        self.assertEqual(3, len(d))
        self.assertEqual(None, d.pop('z', None))
        self.assertRaises(KeyError, d.pop, 'z')
        d.clear()

    def test_popitem(self):
        d = rdict(self.redisugar, 'dict_popitem')
        d.clear()
        d.update([('a', 1), ('b', 2)], c=3, d=4)
        _d = d.copy()
        while len(d) != 0:
            k, v = d.popitem()
            self.assertTrue(k in _d)
            self.assertEqual(v, _d[k])
        self.assertRaises(KeyError, d.popitem)
        d.clear()

    def test_setdefault(self):
        d = rdict(self.redisugar, 'dict_setdefault')
        d.clear()
        d.update([('a', 1), ('b', 2)], c=3, d=4)
        self.assertEqual('1', d.setdefault('a', 2))
        self.assertEqual('2', d.setdefault('z', 2))
        self.assertTrue('z' in d)
        d.clear()

    def test_incr_by(self):
        d = rdict(self.redisugar, 'dict_incr_by')
        d.clear()
        d.update([('a', 1), ('b', 2)], c=3, d=4)
        d.incr_by('a', 2)
        self.assertEqual('3', d['a'])
        d.incr_by_float('a', 0.5)
        self.assertEqual('3.5', d['a'])
        d.clear()

    def test__set_get_del(self):
        d = rdict(self.redisugar, 'dict_set_get_del')
        d.clear()
        d['a'] = 'A'
        self.assertTrue('a' in d)
        self.assertEqual('A', d['a'])
        self.assertFalse('b' in d)
        del d['a']
        self.assertFalse('a' in d)
        d.clear()
