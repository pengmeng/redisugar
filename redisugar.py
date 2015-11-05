# -*- coding: utf-8 -*-
import redis
from collections import Iterable


class RediSugarException(Exception):
    pass


class RediSugar(object):
    _Pool = {}

    @staticmethod
    def getSugar(host='localhost', port=6379, db=0):
        if (host, port, db) not in RediSugar._Pool:
            RediSugar._Pool[(host, port, db)] = redis.ConnectionPool(host=host, port=port, db=db)
        r = redis.Redis(connection_pool=RediSugar._Pool[(host, port, db)])
        try:
            return r.ping() and RediSugar(r)
        except redis.ConnectionError:
            raise RediSugarException('Cannot connect to redis server')

    def __init__(self, _redis):
        self.redis = _redis


class rlist(object):

    def __init__(self, redisugar, key, iterable=None, dtype=str):
        self.redis = redisugar.redis
        self.key = key
        self.dtype = dtype
        if iterable:
            self.extend(iterable)

    def __len__(self):
        return self.redis.llen(self.key)

    def __add__(self, other):
        if not isinstance(other, (list, rlist)):
            raise RediSugarException('TypeError: can only concatenate list or rlist to rlist')
        if isinstance(other, rlist):
            other = other.copy()
        return self.copy() + other

    def __iadd__(self, other):
        if not isinstance(other, (list, rlist)):
            raise RediSugarException('TypeError: can only concatenate list or rlist to rlist')
        if isinstance(other, rlist):
            i, _len = 0, len(other)
            while i < _len:
                self.append(other[i])
                i += 1
        else:
            self.extend(other)
        return self

    def __mul__(self, other):
        if not isinstance(other, int):
            raise RediSugarException('TypeError: can\'t multiply sequence by non-int type')
        return self.copy() * other

    def __imul__(self, other):
        if not isinstance(other, int):
            raise RediSugarException('TypeError: can\'t multiply sequence by non-int type')
        if other <= 0:
            self.clear()
        elif other == 1:
            pass
        else:
            _len = self.__len__()
            for i in range(0, other - 1):
                j = 0
                while j < _len:
                    self.append(self.redis.lindex(self.key, j))
                    j += 1
        return self

    def __iter__(self):
        i = 0
        while i < self.__len__():
            yield self[i]
            i += 1

    def _check_index(self, item):
        if not isinstance(item, int):
            raise RediSugarException('TypeError: list indices must be integers, not ' + str(type(item))[7: -2])
        _len = self.__len__()
        if item >= _len or -item < -_len:
            raise RediSugarException('IndexError: list index out of range')

    def __getitem__(self, key):
        self._check_index(key)
        return self.dtype(self.redis.lindex(self.key, key))

    def __setitem__(self, key, value):
        self._check_index(key)
        self.redis.lset(self.key, key, value)

    def __contains__(self, item):
        i = 0
        while i < self.__len__():
            if self[i] == item:
                return True
            i += 1
        return False

    def append(self, item):
        self.redis.rpush(self.key, item)

    def extend(self, iterable):
        if not isinstance(iterable, Iterable):
            raise RediSugarException('TypeError: \'{0}\' object is not iterable'.format(type(iterable)))
        self.redis.rpush(self.key, *list(iterable))

    def index(self, item, start=0, end=None):
        if start < 0:
            start += self.__len__()
        end = self.__len__() if end is None else end
        if end < 0:
            end += self.__len__()
        if end < start:
            raise RediSugarException('ValueError: {0} is not in list'.format(item))
        i = start
        while i < min(end, self.__len__()):
            if self[i] == item:
                return i
            i += 1
        raise RediSugarException('ValueError: {0} is not in list'.format(item))

    def insert(self, index, item):
        if index < 0:
            index += self.__len__()
        if index >= self.__len__():
            self.append(item)
        else:
            self.redis.rpush(self.key, self.redis.lindex(self.key, -1))
            i = self.__len__() - 2
            while i > index:
                self.redis.lset(self.key, i, self.redis.lindex(self.key, i - 1))
                i -= 1
            self.redis.lset(self.key, index, item)

    def pop(self, pos=-1):
        if pos not in [0, -1]:
            raise RediSugarException('SyntaxError: pos can only be 0 or -1 (head or tail)')
        if self.__len__() == 0:
            raise RediSugarException('IndexError: pop from empty list')
        if pos == 0:
            return self.dtype(self.redis.lpop(self.key))
        elif pos == -1:
            return self.dtype(self.redis.rpop(self.key))

    def push(self, item, pos=-1):
        if pos not in [0, -1]:
            raise RediSugarException('SyntaxError: pos can only be 0 or -1 (head or tail)')
        if pos == 0:
            self.redis.lpush(self.key, item)
        elif pos == -1:
            self.redis.rpush(self.key, item)

    def remove(self, item, count=1):
        flag = self.redis.lrem(self.key, item, count)
        if flag == 0:
            raise RediSugarException('ValueError: rlist.remove(x): {0} not in rlist'.format(item))

    def reverse(self):
        tmp_key = self.key + '-tmp'
        while self.__len__() != 0:
            tmp = self.redis.rpop(self.key)
            self.redis.rpush(tmp_key, tmp)
        self.redis.rename(tmp_key, self.key)

    def sort(self, reverse=False, alpha=False):
        self.redis.sort(self.key, alpha=alpha, desc=reverse, store=self.key)

    def copy(self):
        temp = self.redis.lrange(self.key, 0, -1)
        return map(self.dtype, temp)

    def clear(self):
        self.redis.delete(self.key)


class rset(object):
    pass


class rdict(object):
    pass


class rstr(object):
    pass
