# -*- coding: utf-8 -*-
__author__ = 'mengpeng'
import warnings
import redis


class RediSugarException(Exception):
    pass


class RediSugar(object):
    _Pool = None

    @staticmethod
    def getSugar():
        if not RediSugar._Pool:
            RediSugar._Pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
        r = redis.Redis(connection_pool=RediSugar._Pool)
        try:
            return r.ping() and RediSugar(r)
        except redis.ConnectionError:
            raise RediSugarException('Connect to redis server failed')

    def __init__(self, _redis):
        self.redis = _redis


class rlist(object):

    def __init__(self, redisugar, key, iterable=None):
        self.redis = redisugar.redis
        self.key = key
        if iterable:
            self.redis.rpush(self.key, *list(iterable))

    def __len__(self):
        return self.redis.llen(self.key)

    def __add__(self, other):
        if not isinstance(other, list) or not isinstance(other, rlist):
            raise RediSugarException('TypeError: can only concatenate list or rlist to rlist')
        if isinstance(other, rlist):
            other = other.copy()
        return self.copy() + other

    def __iadd__(self, other):
        if not isinstance(other, list) or not isinstance(other, rlist):
            raise RediSugarException('TypeError: can only concatenate list or rlist to rlist')
        if isinstance(other, rlist):
            other = other.copy()
        # avoid copy into memory later
        self.extend(other)

    def __mul__(self, other):
        if not isinstance(other, int):
            raise RediSugarException('TypeError: can\'t multiply sequence by non-int type')
        return self.copy() * other

    def __imul__(self, other):
        if not isinstance(other, int):
            raise RediSugarException('TypeError: can\'t multiply sequence by non-int type')
        # avoid copy into memory later
        if other <= 0:
            self.clear()
        elif other == 1:
            pass
        else:
            tmp = self.copy()
            for i in range(0, other - 1):
                self.extend(tmp)

    def __iter__(self):
        _len = self.__len__()
        for i in range(0, _len):
            yield self.redis.lindex(self.key, i)
            # _len = self.__len__() # in case of length of the list is changed

    def _check_index(self, item):
        if not isinstance(item, int):
            raise RediSugarException('TypeError: list indices must be integers, not ' + str(type(item))[7: -2])
        _len = self.__len__()
        if item >= _len or -item < -_len:
            raise RediSugarException('IndexError: list index out of range')

    def __getitem__(self, key):
        self._check_index(key)
        return self.redis.lindex(self.key, key)

    def __setitem__(self, key, value):
        self._check_index(key)
        self.redis.lset(self.key, key, value)

    def __contains__(self, item):
        warnings.warn('Using of this function is NOT encouraged.')
        tmp = self.copy()
        flag = item in tmp
        del tmp
        return flag

    def append(self, item):
        self.redis.rpush(self.key, item)

    def extend(self, iterable):
        self.redis.rpush(self.key, *list(iterable))

    def index(self, item):
        warnings.warn('Using of this function is NOT encouraged.')
        tmp = self.copy()
        i = tmp.index(item)
        del tmp
        return i

    def insert(self, pivot, item, pos='after'):
        if pos not in ['after', 'before']:
            raise RediSugarException('SyntaxError: pos can only be after or before')
        self.redis.linsert(self.key, pos, pivot, item)

    def pop(self, pos=-1):
        if pos not in [0, -1]:
            raise RediSugarException('SyntaxError: pos can only be 0 or -1 (head or tail)')
        if self.__len__() == 0:
            raise RediSugarException('IndexError: pop from empty list')
        if pos == 0:
            self.redis.lpop(self.key)
        elif pos == -1:
            self.redis.rpop(self.key)

    def popall(self):
        _len = self.__len__()
        if _len == 0:
            raise RediSugarException('IndexError: pop from empty list')
        temp = self.copy()
        self.clear()
        return temp

    def push(self, item, pos=-1):
        if pos not in [0, -1]:
            raise RediSugarException('SyntaxError: pos can only be 0 or -1 (head or tail)')
        if pos == 0:
            self.redis.lpush(self.key, item)
        elif pos == -1:
            self.redis.rpush(self.key, item)

    def remove(self, item, count=1):
        self.redis.lrem(self.key, item, count)

    def reverse(self):
        tmp_key = self.key + '-tmp'
        while self.__len__() != 0:
            self.redis.rpoplpush(self.key, tmp_key)
        self.redis.rename(tmp_key, self.key)

    def sort(self, reverse=False, alpha=False, inplace=False):
        if inplace:
            self.redis.sort(self.key, alpha=alpha, desc=reverse, store=self.key)
        else:
            self.redis.sort(self.key, alpha=alpha, desc=reverse)

    def copy(self):
        return self.redis.lrange(self.key, 0, -1)

    def clear(self):
        _len = self.__len__()
        self.redis.ltrim(self.key, _len, _len)


class rset(object):
    pass


class rdict(object):
    pass


class rstr(object):
    pass
