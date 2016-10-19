# -*- coding: utf-8 -*-
from unittest import TestCase
from redisugar import RediSugar


class TestRediSugar(TestCase):
    redisugar = None

    @classmethod
    def setUpClass(cls):
        cls.redisugar = RediSugar.getSugar(db=1)
        cls.redisugar.clear()

    def test_get(self):
        self.redisugar['1'] = '1'
        self.assertEqual('1', self.redisugar.get('1'))
        self.assertIsNone(self.redisugar.get('2'))
        self.redisugar.clear()

    def test_pop(self):
        self.redisugar['1'] = '1'
        self.assertEqual('1', self.redisugar.pop('1'))
        self.assertEqual('2', self.redisugar.pop('2', '2'))
        self.assertRaises(KeyError, self.redisugar.pop, '2')
        self.redisugar.clear()

    def test_setdefault(self):
        self.redisugar['1'] = '1'
        self.assertEqual('1', self.redisugar.setdefault('1', '2'))
        self.assertEqual('2', self.redisugar.setdefault('2', '2'))
        self.redisugar.clear()

    def test_getset(self):
        self.redisugar['1'] = '1'
        self.assertEqual('1', self.redisugar.getset('1', '2'))
        self.assertEqual('2', self.redisugar['1'])
        self.redisugar.clear()
