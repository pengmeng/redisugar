# -*- coding: utf-8 -*-
from sugar import (
    RediSugar,
    rlist,
    rdict,
    rset,
    rstr,
    sorted_set,
)

# rzset is a alias of sorted_set
rzset = sorted_set

__all__ = ['RediSugar', 'rlist', 'rdict', 'rset', 'rstr', 'sorted_set', 'rzset']
