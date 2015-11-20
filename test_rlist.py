# -*- coding: utf-8 -*-
from unittest import TestCase
from redisugar import RediSugar
from redisugar import rlist


class TestRlist(TestCase):
    redisugar = None

    @classmethod
    def setUpClass(cls):
        cls.redisugar = RediSugar.getSugar(db=1)

    def test__check_index(self):
        l = rlist(self.__class__.redisugar, 'test_dummy')
        self.assertRaises(TypeError, l._check_index, '1')
        l.clear()
        l.extend([0, 1])
        self.assertRaises(IndexError, l._check_index, 5)

    def test_append(self):
        l = rlist(self.__class__.redisugar, 'test_append', dtype=int)
        l.clear()
        l.append(1)
        l.append(2)
        self.assertEqual([1, 2], l.copy())
        l.clear()

    def test_extend(self):
        l = rlist(self.__class__.redisugar, 'test_extend', dtype=int)
        l.clear()
        l.extend([0, 1, 2, 3])
        self.assertRaises(TypeError, l.extend, 2)
        self.assertEqual([0, 1, 2, 3], l.copy())
        l.clear()

    def test_index(self):
        l = rlist(self.__class__.redisugar, 'test_index', dtype=int)
        l.clear()
        l.extend([0, 1, 2, 3, 3, 4, 3, 4, 5, 6])
        self.assertEqual(2, l.index(2))
        self.assertRaises(ValueError, l.index, 100)
        self.assertRaises(ValueError, l.index, 5, 2, 1)
        self.assertEqual(6, l.index(3, 5))
        self.assertEqual(6, l.index(3, -5))
        self.assertEqual(9, l.index(6, 5))
        self.assertEqual(9, l.index(6, 9))
        l.clear()

    def test_insert(self):
        l = rlist(self.__class__.redisugar, 'test_insert', dtype=int)
        l.clear()
        l.extend([0, 1, 2, 3])
        l.insert(1, 100)
        self.assertEqual([0, 100, 1, 2, 3], l.copy())
        l.insert(10, 100)
        self.assertEqual([0, 100, 1, 2, 3, 100], l.copy())
        l.insert(-1, 99)
        self.assertEqual([0, 100, 1, 2, 3, 99, 100], l.copy())
        l.clear()

    def test_pop(self):
        l = rlist(self.__class__.redisugar, 'test_pop', dtype=int)
        l.clear()
        self.assertRaises(IndexError, l.pop)
        self.assertRaises(IndexError, l.pop, 10)
        l.extend([0, 1, 2, 3])
        self.assertEqual(3, l.pop())
        self.assertEqual(2, l.pop())
        self.assertEqual(0, l.pop(0))

    def test_push(self):
        l = rlist(self.__class__.redisugar, 'test_push', dtype=int)
        l.clear()
        self.assertRaises(ValueError, l.push, 10, 10)
        l.push(1)
        l.push(2)
        l.push(0, pos=0)
        self.assertEqual([0, 1, 2], l.copy())
        l.clear()

    def test_remove(self):
        l = rlist(self.__class__.redisugar, 'test_remove', dtype=int)
        l.clear()
        l.extend([5, 4, 3, 2, 1, 1, 1])
        l.remove(5)
        self.assertEqual(6, len(l))
        l.remove(1, 0)
        self.assertEqual(3, len(l))
        self.assertRaises(ValueError, l.remove, 100)
        l.clear()

    def test_reverse(self):
        l = rlist(self.__class__.redisugar, 'test_reverse', dtype=int)
        l.clear()
        l.extend([5, 4, 3, 2, 1])
        l.reverse()
        self.assertEqual(list(range(1, 6)), l.copy())
        l.clear()

    def test_sort(self):
        l = rlist(self.__class__.redisugar, 'test_sort', dtype=int)
        l.clear()
        l.extend([5, 4, 3, 2, 1])
        l.sort()
        self.assertEqual(list(range(1, 6)), l.copy())
        l.clear()

    def test_copy(self):
        l = rlist(self.__class__.redisugar, 'test_copy', [0, 1, 2, 3], dtype=int)
        self.assertEqual([0, 1, 2, 3], l.copy())
        l.clear()

    def test_clear(self):
        l = rlist(self.__class__.redisugar, 'test_clear', [0, 1, 2, 3])
        self.assertEqual(4, len(l))
        l.clear()
        self.assertEqual(0, len(l))

    def test___getitem__(self):
        l = rlist(self.__class__.redisugar, 'test_getitem', dtype=int)
        l.clear()
        l.extend([0, 1, 2, 3])
        self.assertEqual(1, l[1])
        self.assertEqual(3, l[3])
        self.assertEqual(3, l[-1])
        self.assertEqual(2, l[-2])
        self.assertEqual([], l[3:2])
        self.assertEqual([0, 1, 2], l[:3])
        self.assertEqual([0, 1, 2], l[0:3])
        self.assertEqual([0, 1, 2], l[:-1])
        self.assertEqual([0, 1], l[:-2])
        self.assertEqual([0, 1, 2, 3], l[:])
        self.assertEqual([0, 2], l[::2])
        self.assertEqual([3, 2, 1, 0], l[::-1])
        self.assertEqual([3, 1], l[::-2])
        l.clear()

    def test___setitem__(self):
        l = rlist(self.__class__.redisugar, 'test_setitem', dtype=int)
        l.clear()
        l.extend([0, 1, 2, 3])
        l[2] = 100
        l[-1] = 100
        self.assertEqual(100, l[2])
        self.assertEqual(100, l[3])
        self.assertEqual([0, 1, 100, 100], l.copy())
        l[1:2] = [99]
        self.assertEqual([0, 99, 100, 100], l.copy())
        l[1:] = [0, 1, 2, 3, 4, 5]
        self.assertEqual([0, 0, 1, 2, 3, 4, 5], l.copy())
        l[0:2] = [1, 2, 3, 4]
        self.assertEqual([1, 2, 3, 4, 1, 2, 3, 4, 5], l.copy())
        l[0:7:2] = [9, 9, 9, 9]
        self.assertEqual([9, 2, 9, 4, 9, 2, 9, 4, 5], l.copy())
        l[:] = []
        self.assertEqual([], l.copy())
        l.clear()

    def test___contains__(self):
        l = rlist(self.__class__.redisugar, 'test_contains', dtype=int)
        l.clear()
        l.extend([0, 1, 2, 3])
        self.assertTrue(1 in l)
        self.assertFalse(5 in l)
        l.clear()

    def test___add__(self):
        l1 = rlist(self.__class__.redisugar, 'test_add_1', dtype=int)
        l1.clear()
        l1.extend([0, 1, 2])
        l2 = rlist(self.__class__.redisugar, 'test_add_2', dtype=int)
        l2.clear()
        l2.extend([3, 4, 5])
        self.assertEqual(list(range(0, 6)), l1 + l2)
        self.assertEqual(list(range(0, 6)), l1 + [3, 4, 5])
        l1.clear()
        l2.clear()

    def test___iadd__(self):
        l1 = rlist(self.__class__.redisugar, 'test_iadd_1', dtype=int)
        l1.clear()
        l1.extend([0, 1, 2])
        l2 = rlist(self.__class__.redisugar, 'test_iadd_2', dtype=int)
        l2.clear()
        l2.extend([3, 4, 5])
        l1 += l2
        self.assertEqual(list(range(0, 6)), l1.copy())
        l1 += [6, 7, 8]
        self.assertEqual(list(range(0, 9)), l1.copy())
        l2 += l2
        self.assertEqual([3, 4, 5, 3, 4, 5], l2.copy())
        l1.clear()
        l2.clear()

    def test___mul__(self):
        l1 = rlist(self.__class__.redisugar, 'test_mul', dtype=int)
        l1.clear()
        l1.extend([0, 1, 2])
        self.assertEqual([0, 1, 2] * 3, l1 * 3)
        self.assertEqual([], l1 * 0)
        l1.clear()

    def test___imul__(self):
        l1 = rlist(self.__class__.redisugar, 'test_imul', dtype=int)
        l1.clear()
        l1.extend([0, 1, 2])
        l1 *= 3
        self.assertEqual([0, 1, 2] * 3, l1.copy())
        l1 *= 0
        self.assertEqual([], l1.copy())
        l1.clear()

    def test___iter__(self):
        l = rlist(self.__class__.redisugar, 'test_iter', dtype=int)
        l.clear()
        l.extend([0, 1, 2, 3, 4])
        tmp = []
        for x in iter(l):
            tmp.append(x)
        self.assertEqual(tmp, l.copy())
        l.clear()

    def test_count(self):
        l = rlist(self.__class__.redisugar, 'test_count', dtype=int)
        l.clear()
        l.extend([0, 1, 2, 3, 4, 4, 4, 4])
        self.assertEqual(1, l.count(1))
        self.assertEqual(4, l.count(4))

    def test___delitem__(self):
        l = rlist(self.__class__.redisugar, 'test_del', dtype=int)
        l.clear()
        l.extend([0, 1, 2, 3, 4, 4, 4, 4])
        del l[0]
        self.assertEqual([1, 2, 3, 4, 4, 4, 4], l.copy())
        del l[-1]
        self.assertEqual([1, 2, 3, 4, 4, 4], l.copy())
        del l[3]
        self.assertEqual([1, 2, 3, 4, 4], l.copy())
        del l[:]
        self.assertEqual([], l.copy())
        l.extend([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        del l[1:6:2]
        self.assertEqual([0, 2, 4, 6, 7, 8, 9], l.copy())
        del l[2:6]
        self.assertEqual([0, 2, 9], l.copy())
        del l[:-1]
        self.assertEqual([9], l.copy())
        del l[-1]
        self.assertEqual([], l.copy())
        l.clear()
