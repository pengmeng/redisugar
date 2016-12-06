# -*- coding: utf-8 -*-
"""
Some small helper functions
"""


def get_type(obj):
    return str(type(obj))[7: -2]


def raise_not_hashable(item):
    hash(item)