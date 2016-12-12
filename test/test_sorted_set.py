# -*- coding: utf-8 -*-
from unittest import TestCase
from redisugar import RediSugar, sorted_set


class TestSorted_set(TestCase):
    redisugar = None
    data1 = [('a', 1), ('b', 2), ('c', 3), ('d', 4)]
    data2 = [('c', 3), ('d', 4), ('e', 5), ('f', 6)]

    @classmethod
    def setUpClass(cls):
        cls.redisugar = RediSugar.get_sugar(db=1)

    @classmethod
    def tearDownClass(cls):
        keys = [key for key in list(cls.redisugar.redis.scan_iter()) if key.startswith('zset_')]
        if keys:
            cls.redisugar.redis.delete(*keys)

    def test_intersection_store(self):
        z1 = sorted_set(self.redisugar, 'zset_inter_1', self.data1)
        z2 = sorted_set(self.redisugar, 'zset_inter_2', self.data2)
        z = sorted_set.intersection_store(self.redisugar, 'zset_inter_dest', [z1, z2.key])
        self.assertEqual(2, len(z))
        self.assertDictEqual({'c': 6, 'd': 8}, dict(z.copy()))
        self.assertRaises(ValueError, sorted_set.intersection_store, self.redisugar, 'zset_inter_dest', [z1, z2],
                          overwrite=False)
        z = sorted_set.intersection_store(self.redisugar, 'zset_inter_dest', [z1, z2.key], [0, 1], 'MAX')
        self.assertEqual(2, len(z))
        self.assertDictEqual({'c': 3, 'd': 4}, dict(z.copy()))
        z1.clear()
        z2.clear()
        z.clear()

    def test_union_store(self):
        z1 = sorted_set(self.redisugar, 'zset_union_1', self.data1)
        z2 = sorted_set(self.redisugar, 'zset_union_2', self.data2)
        z = sorted_set.union_store(self.redisugar, 'zset_union_dest', [z1, z2])
        self.assertEqual(6, len(z))
        z1.clear()
        z2.clear()
        z.clear()

    def test__make_writable(self):
        self.assertRaises(ValueError, sorted_set._make_writable, 'a')
        d = {'a': 1, 'b': 2}
        self.assertDictEqual(sorted_set._make_writable('a', 1, 'b', 2), d)
        self.assertDictEqual(sorted_set._make_writable(a=1, b=2), d)
        self.assertDictEqual(sorted_set._make_writable([('a', 1), ('b', 2)]), d)
        self.assertDictEqual(sorted_set._make_writable('a', 1, b=2), d)

    def test__contains(self):
        z = sorted_set(self.redisugar, 'zset_contains', self.data1)
        self.assertIn('a', z)
        self.assertNotIn('z', z)
        z.add(z=0)
        self.assertIn('z', z)
        z.clear()

    def test_copy(self):
        z = sorted_set(self.redisugar, 'zset_copy', self.data1)
        self.assertDictEqual(dict(self.data1), dict(z.copy()))
        z.clear()

    def test_discard(self):
        z = sorted_set(self.redisugar, 'zset_discard', self.data1)
        z.discard('z')
        z.discard('a')
        self.assertNotIn('a', z)
        z.clear()

    def test_remove(self):
        z = sorted_set(self.redisugar, 'zset_remove', self.data1)
        self.assertRaises(KeyError, z.remove, 'z')
        z.remove('a')
        self.assertNotIn('a', z)
        z.clear()

    def test_add(self):
        z = sorted_set(self.redisugar, 'zset_add')
        z.add('a', 1, b=2)
        z.add([('c', 3)], d=4)
        self.assertDictEqual(dict(self.data1), dict(z.copy()))
        # test for __setitems__
        z['e'] = 5
        self.assertIn('e', z)
        self.assertRaises(TypeError, z.__setitem__, 0, 0)
        z.clear()

    def test_score(self):
        z = sorted_set(self.redisugar, 'zset_score', self.data1)
        self.assertEqual(1.0, z.score('a'))
        self.assertListEqual([2.0, 3.0], z.score('b', 'c'))
        z.clear()

    def test_incr_by(self):
        z = sorted_set(self.redisugar, 'zset_incrby', self.data1)
        z.incr_by('a', 1)
        self.assertEqual(2.0, z['a'])
        z.incr_by('a', 0.5)
        self.assertEqual(2.5, z['a'])
        # increase a non-exists key
        z.incr_by('z', 1)
        self.assertEqual(1.0, z['z'])
        z.clear()

    def test_rank(self):
        z = sorted_set(self.redisugar, 'zset_rank', self.data1)
        self.assertEqual(0, z.rank('a'))
        self.assertEqual(3, z.rank('a', reverse=True))
        self.assertIsNone(z.rank('z'))
        z.clear()

    def test_count(self):
        z = sorted_set(self.redisugar, 'zset_count', self.data1)
        self.assertEqual(2, z.count(0.5, 2.5))
        self.assertEqual(1, z.count(1, 1))
        self.assertEqual(0, z.count(5, 5))
        z.clear()

    def test_range(self):
        z = sorted_set(self.redisugar, 'zset_range', self.data1)
        self.assertEqual([], z.range(0, 0))
        self.assertListEqual(['b', 'c'], z.range(1, 3))
        self.assertListEqual([], z.range(1, 1))
        self.assertListEqual(['d', 'c'], z.range(0, 2, reverse=True))
        # test for __getitems__
        self.assertEqual([], z[:0])
        self.assertEqual('b', z[1])
        self.assertListEqual(['a', 'b'], z[:2])
        self.assertListEqual(['a', 'c'], z[:3:2])
        z.clear()

    def test_range_by_score(self):
        z = sorted_set(self.redisugar, 'zset_range_by_score', self.data1)
        self.assertListEqual(['b', 'c'], z.range_by_score(2, 3))
        self.assertListEqual(['c', 'b'], z.range_by_score(3, 2, reverse=True))
        z.clear()

    def test_remove_range(self):
        z = sorted_set(self.redisugar, 'zset_remove_range', self.data1)
        self.assertEqual(0, z.remove_range(0, 0))
        self.assertEqual(2, z.remove_range(0, 2))
        self.assertEqual(2, len(z))
        self.assertEqual(1, z.remove_range(0, -1))
        self.assertEqual(1, z.remove_range(0, 1))
        # test for __delitems__
        z.add(self.data1)
        del z[0:0]
        self.assertEqual(4, len(z))
        del z[:3:2]
        self.assertEqual(2, len(z))
        self.assertIn('b', z)
        self.assertIn('d', z)
        del z[-1:]
        self.assertEqual(1, len(z))
        self.assertIn('b', z)
        del z[-1:-1]
        self.assertIn('b', z)
        z.clear()

    def test_remove_range_by_score(self):
        z = sorted_set(self.redisugar, 'zset_remove_range_by_score', self.data1)
        self.assertEqual(0, z.remove_range_by_score(0.5, 0.5))
        self.assertEqual(1, z.remove_range_by_score(1, 1))
        self.assertEqual(3, z.remove_range_by_score(0, 4))
        z.clear()