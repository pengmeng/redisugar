# -*- coding: utf-8 -*-
from unittest import TestCase

from redisugar import RediSugar, rset


class TestRset(TestCase):
    redisugar = None

    @classmethod
    def setUpClass(cls):
        cls.redisugar = RediSugar.get_sugar(db=1)

    @classmethod
    def tearDownClass(cls):
        keys = [key for key in list(cls.redisugar.redis.scan_iter()) if key.startswith('set_')]
        if keys:
            cls.redisugar.redis.delete(*keys)

    def test__init(self):
        self.assertRaises(TypeError, rset, self.redisugar, 'set_dummy', 1)
        self.assertRaises(TypeError, rset, self.redisugar, 'set_dummy', [1, 2, []])
        s = rset(self.redisugar, 'set_init', [1, 2, 3, 4])
        self.assertEqual(4, len(s))
        self.assertSetEqual({'1', '2', '3', '4'}, s.copy())
        s.clear()
        self.assertEqual(0, len(s))

    def test__make_sets(self):
        l = [set(), frozenset(), [1, 2, 3]]
        for item in rset._make_sets(l):
            self.assertIsInstance(item, (set, frozenset, rset))
        l.append(1)
        self.assertRaises(TypeError, rset._make_sets, l)

    def test__cmp_length(self):
        s = rset(self.redisugar, 'set__cmp_length', [1, 2, 3, 4])
        _s = {1, 2, 3}
        small, large = s._cmp_length(_s)
        self.assertIs(small, _s)
        self.assertIs(large, s)
        _s = {1, 2, 3, 4, 5}
        small, large = s._cmp_length(_s)
        self.assertIs(small, s)
        self.assertIs(large, _s)
        s.clear()

    def test__contains(self):
        s = rset(self.redisugar, 'set__contains', [1, 2, 3, 4])
        self.assertIn('1', s)
        self.assertNotIn('5', s)
        s.clear()

    def test__iter(self):
        s = rset(self.redisugar, 'set__iter', [1, 2, 3, 4])
        s_list = list(iter(s))
        self.assertListEqual(['1', '2', '3', '4'], s_list)
        s.clear()

    def test_add(self):
        s = rset(self.redisugar, 'set_add')
        s.add(1)
        self.assertIn('1', s)
        self.assertRaises(TypeError, s.add, [])
        self.assertEqual(1, len(s))
        s.clear()

    def test_discard(self):
        s = rset(self.redisugar, 'set_discard', [1, 2, 3, 4])
        s.discard('4')
        self.assertEqual(3, len(s))
        s.discard('5')
        self.assertEqual(3, len(s))
        s.clear()

    def test_remove(self):
        s = rset(self.redisugar, 'set_remove', [1, 2, 3, 4])
        s.remove('1')
        self.assertEqual(3, len(s))
        self.assertRaises(KeyError, s.remove, '5')
        s.clear()

    def test_pop(self):
        s = rset(self.redisugar, 'set_pop', [1, 2, 3, 4])
        v = s.pop()
        self.assertEqual(3, len(s))
        self.assertNotIn(v, s)
        s.clear()
        self.assertRaises(KeyError, s.pop)

    def test_union(self):
        s = rset(self.redisugar, 'set_union', [1, 2])
        _s = rset(self.redisugar, 'set_temp', [3, 4])
        t = {'1', '2', '3', '4'}
        # union
        self.assertSetEqual(t, s.union(['3'], set('4')))
        self.assertSetEqual(t, s.union(_s))
        # __or__
        self.assertSetEqual(t, s | {'3', '4'})
        self.assertEqual(t, s | {'3'} | {'4'})
        self.assertSetEqual(t, s | _s)
        self.assertRaises(TypeError, lambda: s | ['3'])
        # __ror__
        self.assertSetEqual(t, {'3', '4'} | s)
        self.assertEqual(t, {'3'} | s | {'4'})
        self.assertRaises(TypeError, lambda: 1 | s)
        # __ior__
        s |= set()
        s |= {'3'} | {'4'}
        self.assertSetEqual(t, s)
        s.discard('3')
        s.discard('4')
        s |= _s
        self.assertSetEqual(t, s)
        s.clear()
        _s.clear()

    def test_update(self):
        s = rset(self.redisugar, 'set_update', [1, 2])
        _s = rset(self.redisugar, 'set_temp', [3, 4])
        t = {'1', '2', '3', '4'}
        s.update(['3'], {'4'})
        s.update(set())
        self.assertSetEqual(t, s.copy())
        s.discard('3')
        s.discard('4')
        s.update(_s)
        self.assertSetEqual(t, s)
        s.clear()
        _s.clear()

    def test_intersection(self):
        s = rset(self.redisugar, 'set_intersection', [1, 2, 3, 4])
        _s = rset(self.redisugar, 'set_temp', [3, 4])
        t = {'3', '4', '5', '6'}
        # intersection
        self.assertSetEqual({'3', '4'}, s.intersection(t))
        self.assertSetEqual({'3', '4'}, s.intersection(_s))
        self.assertSetEqual({'1'}, s.intersection(['1', '2'], set('1')))
        self.assertSetEqual(set(), s.intersection(set()))
        # __and__
        self.assertSetEqual({'3', '4'}, s & _s)
        self.assertSetEqual({'3'}, s & t & {'3'})
        self.assertSetEqual(set(), s & {'5'})
        self.assertRaises(TypeError, lambda: s & 1)
        # __rand__
        self.assertSetEqual({'3', '4'}, t & s)
        self.assertSetEqual(set(), set() & s)
        # __iand__
        s &= _s
        self.assertSetEqual({'3', '4'}, s)
        s &= {'4', '5', '6'}
        self.assertSetEqual({'4'}, s)
        s &= set()
        self.assertEqual(0, len(s))
        s.clear()
        _s.clear()

    def test_intersection_update(self):
        s = rset(self.redisugar, 'set_intersection_upate', [1, 2, 3, 4])
        _s = rset(self.redisugar, 'set_temp', [3, 4])
        s.intersection_update(['3', '4'], {'4'})
        self.assertSetEqual({'4'}, s)
        s.intersection_update(set())
        self.assertEqual(0, len(s))
        s.add('3')
        s.add('5')
        s.intersection_update(_s)
        self.assertSetEqual({'3'}, s)
        s.clear()
        _s.clear()

    def test_difference(self):
        s = rset(self.redisugar, 'set_difference', [1, 2, 3, 4])
        _s = rset(self.redisugar, 'set_temp', [1, 2])
        t = {'3', '4', '5', '6'}
        # difference
        self.assertSetEqual({'1', '2'}, s.difference(['3'], {'4'}))
        self.assertSetEqual({'3', '4'}, s.difference(_s))
        self.assertSetEqual({'1', '2'}, s.difference(t))
        self.assertSetEqual(s, s.difference(set()))
        # __sub__
        self.assertSetEqual({'1', '2'}, s - {'3', '4', '5'})
        self.assertSetEqual({'1', '2'}, s - t)
        self.assertSetEqual({'1'}, s - {'2'} - {'3', '4'})
        self.assertSetEqual({'3', '4'}, s - _s)
        # __rsub__
        self.assertSetEqual(set(), set() - s)
        self.assertSetEqual(set(), _s - s)
        s.add('0')
        self.assertSetEqual({'5', '6'}, t - s)
        # __isub__
        s -= _s
        self.assertSetEqual({'3', '4', '0'}, s)
        s -= t
        self.assertSetEqual({'0'}, s)
        s -= set()
        self.assertSetEqual({'0'}, s)
        s.clear()
        _s.clear()

    def test_difference_update(self):
        s = rset(self.redisugar, 'set_difference_update', [1, 2, 3, 4])
        _s = rset(self.redisugar, 'set_temp', [1, 2])
        t = {'3', '4', '5', '6'}
        s.difference_update(_s)
        self.assertSetEqual({'3', '4'}, s)
        s.difference_update(t, set())
        self.assertSetEqual(set(), s)
        s.add('0')
        s.difference_update(['0'])
        self.assertEqual(0, len(s))
        s.clear()
        _s.clear()

    def test_symmetric_difference(self):
        s = rset(self.redisugar, 'set_symmetric_difference', [1, 2, 3, 4])
        _s = rset(self.redisugar, 'set_temp', [3, 4, 5, 6])
        t = {'0', '1', '2'}
        # symmetric_difference
        self.assertSetEqual({'1', '2', '5', '6'}, s.symmetric_difference(_s))
        self.assertSetEqual({'0', '3', '4'}, s.symmetric_difference(t))
        self.assertSetEqual(s, s.symmetric_difference(set()))
        # __xor__, __rxor__
        self.assertSetEqual({'1'}, s ^ _s ^ {'2', '5', '6'})
        self.assertSetEqual({'0', '5', '6'}, s ^ t ^ _s)
        # __ixor__
        t ^= s
        self.assertSetEqual({'0', '3', '4'}, t)
        t = {'0', '1', '2'}
        s ^= _s
        self.assertSetEqual({'1', '2', '5', '6'}, s)
        s ^= t
        self.assertSetEqual({'0', '5', '6'}, s)
        s ^= s
        self.assertEqual(0, len(s))
        s.clear()
        _s.clear()

    def test_symmetric_difference_update(self):
        s = rset(self.redisugar, 'set_symmetric_difference_update', [1, 2, 3, 4])
        _s = rset(self.redisugar, 'set_temp', [3, 4, 5, 6])
        t = {'0', '1', '2'}
        s.symmetric_difference_update(_s)
        self.assertSetEqual({'1', '2', '5', '6'}, s)
        s.symmetric_difference_update(t)
        self.assertSetEqual({'0', '5', '6'}, s)
        s.symmetric_difference_update(s)
        self.assertEqual(0, len(s))
        s.clear()
        _s.clear()

    def test_isdisjoint(self):
        s = rset(self.redisugar, 'set_isdisjoint', [1, 2, 3, 4])
        self.assertFalse(s.isdisjoint({'1', '2'}))
        self.assertTrue(s.isdisjoint(set()))
        self.assertTrue(s.isdisjoint(['5', '6']))
        self.assertFalse(s.isdisjoint(dict.fromkeys(['1', '2'])))
        self.assertRaises(TypeError, s.isdisjoint, 1)
        _s = rset(self.redisugar, 'set_temp', ['5', '6'])
        self.assertTrue(s.isdisjoint(_s))
        s.clear()
        _s.clear()

    def test_issubset(self):
        s = rset(self.redisugar, 'set_issubset', [1, 2])
        self.assertFalse(s.issubset({'1'}))
        self.assertTrue(s.issubset(['1', '2']))
        self.assertTrue(s.issubset(dict.fromkeys(['1', '2', '3'])))
        self.assertRaises(TypeError, s.issubset, 1)
        _s = rset(self.redisugar, 'set_temp')
        self.assertFalse(s.issubset(_s))
        s.clear()
        _s.clear()

    def test_issuperset(self):
        s = rset(self.redisugar, 'set_issuperset', [1, 2, 3, 4])
        self.assertFalse(s.issuperset({'1', '5'}))
        self.assertTrue(s.issuperset(['1', '2']))
        self.assertTrue(s.issuperset(dict.fromkeys(['1', '2', '3', '4'])))
        self.assertRaises(TypeError, s.issuperset, 1)
        _s = rset(self.redisugar, 'set_temp')
        self.assertTrue(s.issuperset(_s))
        s.clear()
        _s.clear()
