# -*- coding: utf-8 -*-
import redis
from collections import Iterable
from collections import Mapping


class RediSugar(object):
    _Pool = {}

    @classmethod
    def getSugar(cls, host='localhost', port=6379, db=0):
        if (host, port, db) not in RediSugar._Pool:
            cls._Pool[(host, port, db)] = redis.ConnectionPool(host=host, port=port, db=db)
        r = redis.Redis(connection_pool=cls._Pool[(host, port, db)])
        try:
            return r.ping() and cls(r)
        except redis.ConnectionError:
            raise RuntimeError('Cannot connect to redis server')

    def __init__(self, redis_instance):
        self.redis = redis_instance


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
            raise TypeError('can only concatenate list or rlist to rlist')
        if isinstance(other, rlist):
            other = other.copy()
        return self.copy() + other

    def __iadd__(self, other):
        if not isinstance(other, (list, rlist)):
            raise TypeError('can only concatenate list or rlist to rlist')
        if isinstance(other, rlist):
            i, _len = 0, len(other)
            while i < _len:
                self.append(other[i])
                i += 1
        else:
            self.redis.rpush(self.key, *other)
        return self

    def __mul__(self, other):
        if not isinstance(other, int):
            raise TypeError('can\'t multiply sequence by non-int of type ' + str(type(other))[7: -2])
        return self.copy() * other

    def __rmul__(self, other):
        return self.__mul__(other)

    def __imul__(self, other):
        if not isinstance(other, int):
            raise TypeError('can\'t multiply sequence by non-int of type ' + str(type(other))[7: -2])
        if other <= 0:
            self.clear()
        elif other == 1:
            pass
        else:
            _len = self.__len__()
            for i in range(0, other - 1):
                j = 0
                while j < _len:
                    self.append(self._read(j))
                    j += 1
        return self

    def __iter__(self):
        i = 0
        while i < self.__len__():
            yield self[i]
            i += 1

    def __format__(self, format_spec):
        if isinstance(format_spec, unicode):
            return unicode(str(self))
        else:
            return str(self)

    def __repr__(self):
        return '<redisugar.rlist object with key: ' + self.key + '>'

    def __str__(self):
        return str(self.copy())

    def _check_index(self, item):
        if not isinstance(item, int):
            raise TypeError('list indices must be integers, not ' + str(type(item))[7: -2])
        _len = self.__len__()
        if item >= _len or -item < -_len:
            raise IndexError('list index out of range')

    def _read(self, index):
        return self.redis.lindex(self.key, index)

    def _write(self, index, value):
        self.redis.lset(self.key, index, value)

    def __getitem__(self, key):
        if isinstance(key, slice):
            start, stop, step = key.indices(self.__len__())
            result = []
            for i in range(start, stop, step):
                result.append(self[i])
            return result
        else:
            self._check_index(key)
            return self.dtype(self._read(key))

    def __setitem__(self, key, value):
        if isinstance(key, slice):
            if not isinstance(value, Iterable):
                raise TypeError('can only assign an iterable')
            value = list(value)
            start, stop, step = key.indices(self.__len__())
            if step == 1:
                k_i, k_len = 0, len(value)
                for i in range(start, stop, step):
                    if k_i < k_len:
                        self._write(i, value[k_i])
                        k_i += 1
                    else:
                        break
                else:
                    if k_len - k_i == 0:
                        return
                    jump = k_len - k_i
                    self.extend([self._read(-1)] * jump)
                    i = self.__len__() - 1
                    while i - jump >= stop:
                        self._write(i, self._read(i - jump))
                        i -= 1
                    k_len -= 1
                    while i >= stop:
                        self._write(i, value[k_len])
                        i -= 1
                        k_len -= 1
                if start + k_i < stop:
                    del self[start + k_i: stop]
            else:
                index = range(start, stop, step)
                if len(index) != len(value):
                    raise ValueError('attempt to assign sequence of size {0} '
                                     'to extended slice of size {1}'.format(len(value), len(index)))
                else:
                    for i, item in zip(index, value):
                        self._write(i, item)
        else:
            self._check_index(key)
            self._write(key, value)

    def __delitem__(self, key):
        if isinstance(key, slice):
            start, stop, step = key.indices(self.__len__())
            to_del = set(range(start, stop, step))
            i, counter = start, 0
            while i < self.__len__():
                if counter == len(to_del):
                    break
                if i in to_del:
                    counter += 1
                else:
                    self._write(i - counter, self._read(i))
                i += 1
            while i < self.__len__():
                self._write(i - counter, self._read(i))
                i += 1
            self.redis.ltrim(self.key, 0, -counter - 1)
        else:
            self._check_index(key)
            if key < 0:
                key += self.__len__()
            if key == 0:
                self.redis.lpop(self.key)
            elif key == self.__len__() - 1:
                self.redis.rpop(self.key)
            else:
                while key < self.__len__() - 1:
                    self._write(key, self._read(key + 1))
                    key += 1
                self.redis.rpop(self.key)

    def __contains__(self, item):
        i = 0
        while i < self.__len__():
            if self[i] == item:
                return True
            i += 1
        return False

    def append(self, item):
        self.redis.rpush(self.key, item)

    def count(self, item):
        acc, i = 0, 0
        while i < self.__len__():
            if self[i] == item:
                acc += 1
            i += 1
        return acc

    def extend(self, iterable):
        if not isinstance(iterable, Iterable):
            raise TypeError('\'{0}\' object is not iterable'.format(type(iterable)))
        self.redis.rpush(self.key, *list(iterable))

    def index(self, item, start=0, stop=-1):
        start, stop, _ = slice(start, stop, 1).indices(self.__len__())
        if stop < start:
            raise ValueError('{0} is not in list'.format(item))
        i = start
        while i <= stop:
            if self[i] == item:
                return i
            i += 1
        raise ValueError('{0} is not in list'.format(item))

    def insert(self, index, item):
        if index < 0:
            index += self.__len__()
        if index == 0:
            self.redis.lpush(self.key, item)
        elif index >= self.__len__():
            self.redis.rpush(self.key, item)
        else:
            self.redis.rpush(self.key, self._read(-1))
            i = self.__len__() - 2
            while i > index:
                self._write(i, self._read(i - 1))
                i -= 1
            self._write(index, item)

    def pop(self, pos=-1):
        if self.__len__() == 0:
            raise IndexError('pop from empty list')
        self._check_index(pos)
        temp = self.dtype(self._read(pos))
        self.__delitem__(pos)
        return temp

    def push(self, item, pos=-1):
        if pos not in [0, -1]:
            raise ValueError('pos can only be 0 or -1 (head or tail)')
        if pos == 0:
            self.redis.lpush(self.key, item)
        elif pos == -1:
            self.redis.rpush(self.key, item)

    def remove(self, item, count=1):
        flag = self.redis.lrem(self.key, item, count)
        if flag == 0:
            raise ValueError('rlist.remove(x): {0} not in rlist'.format(item))

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


class rdict(object):
    """
    redis dict class

    Note:
        - builtin dict methods (viewitems(), viewkeys(), viewvalues()) that return view object are not implemented
        - rdict.copy() method is an alias of rdict.items() method but slightly different from dict.copy()
        - several methods (value_len(), ) are implemented to take advatange of redis-py interfaces

    Warning:
        - key and value only support str type at present, all other type will be converted to str
        - None will be converted to 'None' in redis
    """
    def __init__(self, redisugar, key, *args, **kwargs):
        self.redis = redisugar.redis
        self.key = key
        len_args = len(args)
        if len_args == 1:
            iterable_or_mapping = args[0]
        elif len_args > 1:
            raise TypeError('rdict expected at most 1 argument for initialization, got {0}'.format(len_args))
        else:
            iterable_or_mapping = None
        self._update(iterable_or_mapping, **kwargs)

    def _update(self, iterable_or_mapping, **kwargs):
        if iterable_or_mapping:
            if isinstance(iterable_or_mapping, Iterable):
                for i, each in enumerate(iterable_or_mapping):
                    try:
                        if len(each) != 2:
                            raise ValueError('dictionary update sequence element #{0} has length {1}; 2 is '
                                             'required'.format(i, len(each)))
                        else:
                            self._write(each[0], each[1])
                    except TypeError:
                        raise TypeError('cannot convert dictionary update sequence element #{0} to a '
                                        'sequence'.format(i))
            elif isinstance(iterable_or_mapping, Mapping):
                for k, v in iterable_or_mapping:
                    self._write(k, v)
            else:
                raise TypeError('\'{0}\' object is not iterable'.format(str(type(iterable_or_mapping))[7: -2]))
        if kwargs:
            for k in kwargs:
                self._write(k, kwargs[k])

    def _raise_not_hashable(self, item):
        hash(item)

    def _check_key_exists(self, key):
        if not self.redis.hexists(self.key, key):
            raise KeyError(str(key))

    def _read(self, key):
        return self.redis.hget(self.key, key)

    def _write(self, key, value):
        self.redis.hset(self.key, key, value)

    def _del(self, key):
        self.redis.hdel(self.key, key)

    def __contains__(self, item):
        self._raise_not_hashable(item)
        return self.redis.hexists(self.key, item)

    def __len__(self):
        return self.redis.hlen(self.key)

    def __getitem__(self, item):
        self._raise_not_hashable(item)
        self._check_key_exists(item)
        return self._read(item)

    def __setitem__(self, key, value):
        self._raise_not_hashable(key)
        self._write(key, value)

    def __delitem__(self, key):
        self._raise_not_hashable(key)
        self._check_key_exists(key)
        self._del(key)

    def __iter__(self):
        # optimize later
        return iter(self.keys())

    def __repr__(self):
        return '<redisugar.rdict object with key: ' + self.key + '>'

    def __str__(self):
        return str(self.copy())

    def __format__(self, format_spec):
        if isinstance(format_spec, unicode):
            return unicode(str(self))
        else:
            return str(self)

    def clear(self):
        self.redis.delete(self.key)

    def copy(self):
        """
        alias of rdict.items()
        """
        return self.items()

    @classmethod
    def fromkeys(cls, redisugar, key, seq, value=None):
        rd = cls(redisugar, key)
        for each in seq:
            rd[each] = value
        return rd

    def get(self, key, default=None):
        return self._read(key) if self.__contains__(key) else default

    def keys(self):
        return self.redis.hkeys(self.key)

    def values(self):
        return self.redis.hvals(self.key)

    def items(self):
        return self.redis.hgetall(self.key)

    def iterkeys(self):
        for each in self.redis.hscan_iter(self.key):
            yield each[0]

    def itervalues(self):
        for each in self.redis.hscan_iter(self.key):
            yield each[1]

    def iteritems(self):
        for each in self.redis.hscan_iter(self.key):
            yield each

    def pop(self, key, *defaults):
        if len(defaults) > 1:
            raise TypeError('pop expected at most 2 arguments, got {0}'.format(1 + len(defaults)))
        try:
            value = self.__getitem__(key)
            self.__delitem__(key)
        except KeyError:
            if defaults:
                return defaults[0]
            else:
                raise
        return value

    def popitem(self):
        for key in self.iterkeys():
            break
        else:
            raise KeyError('popitem(): dictionary is empty')
        value = self._read(key)
        self._del(key)
        return key, value

    def setdefault(self, key, value=None):
        if self.__contains__(key):
            return self._read(key)
        else:
            self._write(key, value)
            return str(value)

    def update(self, *args, **kwargs):
        len_args = len(args)
        if len_args == 1:
            iterable_or_mapping = args[0]
        elif len_args > 1:
            raise TypeError('update expected at most 1 (non-keyword) argument, got {0}'.format(len_args))
        else:
            iterable_or_mapping = None
        self._update(iterable_or_mapping, **kwargs)

    def multi_set(self, *args):
        raise NotImplemented

    def multi_get(self, *args):
        raise NotImplemented

    def incr_by(self, key, amount):
        """
        Increase the value by integer amount with given key
        :param key: rdict key
        :param amount: (int) increase amount
        :return: value after increased
        """
        self._raise_not_hashable(key)
        self._check_key_exists(key)
        return self.redis.hincrby(self.key, key, amount)

    def incr_by_float(self, key, amount):
        """
        Increase the value by float amount with given key
        :param key: rdict key
        :param amount: (float) increase amount
        :return: value after increased
        """
        self._raise_not_hashable(key)
        self._check_key_exists(key)
        return self.redis.hincrbyfloat(self.key, key, amount)


class rset(object):
    pass


class rstr(object):
    pass
