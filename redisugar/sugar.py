# -*- coding: utf-8 -*-
import redis
import collections
from collections import Iterable
from collections import Mapping
from utils import *


class RediSugar(object):
    """
    A wrapper for redis.Redis() object, supports database level operations (dict-like operations).
    """
    _Pool = {}
    _STR_SUMMARY_LIMIT = 10

    @classmethod
    def get_sugar(cls, host='localhost', port=6379, db=0):
        if (host, port, db) not in RediSugar._Pool:
            cls._Pool[(host, port, db)] = redis.ConnectionPool(host=host, port=port, db=db)
        r = redis.Redis(connection_pool=cls._Pool[(host, port, db)])
        try:
            return r.ping() and cls(r)
        except redis.ConnectionError:
            raise RuntimeError('Cannot connect to redis server')

    @classmethod
    def set_str_summary_limit(cls, limit):
        if limit <= 3:
            raise ValueError('str summary limit should greater than 3')
        RediSugar._STR_SUMMARY_LIMIT = limit

    @classmethod
    def str_summary_limit(cls):
        return RediSugar._STR_SUMMARY_LIMIT

    def __init__(self, redis_instance):
        self.redis = redis_instance

    def __len__(self):
        """Returns the number of keys in the current database
        :return: redis.dbsize()
        """
        return self.redis.dbsize()

    def __contains__(self, name):
        """Returns a boolean indicating whether key name exists
        :param name: redis db key
        :return: True/False
        """
        return self.redis.exists(name)

    def __setitem__(self, key, value, expire_seconds=None, expire_milliseconds=None, not_exists=False, if_exists=False):
        """Set the value at key name to value
        :param key: redis key
        :param value: value to the key
        """
        if isinstance(value, list):
            rlist(self, key, value)
        elif isinstance(value, dict):
            rdict(self, key, value)
        elif isinstance(value, (set, frozenset)):
            rset(self, key, value)
        else:
            self.redis.set(key, value, expire_seconds, expire_milliseconds, not_exists, if_exists)

    def set(self, key, value, expire_seconds=None, expire_milliseconds=None, not_exists=False, if_exists=False):
        """Alias of redis.set with optional arguments
        :param key: redis key
        :param value: value to the key
        :param expire_seconds: sets an expire flag on key name for ex seconds
        :param expire_milliseconds: sets an expire flag on key name for px milliseconds
        :param not_exists: if True, set the value at key to value if it does not already exist
        :param if_exists:  if True, set the value at key name to value if it already exists
        """
        self.__setitem__(key, value, expire_seconds, expire_milliseconds, not_exists, if_exists)

    def __getitem__(self, key):
        """Return the value at key
        :param key: redis key
        :return: value at the key
        :raise KeyError: when key does not exists
        """
        if not self.__contains__(key):
            raise KeyError(str(key))
        _type = self.redis.type(key)
        if _type == 'list':
            value = rlist(self, key)
        elif _type == 'hash':
            value = rdict(self, key)
        elif _type == 'set':
            value = rset(self, key)
        else:
            value = self.redis.get(key)
        return value

    def __delitem__(self, key):
        """Delete one key from database
        :param key: redis key
        :raise KeyError: when key does not exists
        """
        if not self.__contains__(key):
            raise KeyError(str(key))
        self.redis.delete(key)

    def __iter__(self):
        """Return a generator of current database keys
        :return: generator object
        """
        return self.redis.scan_iter()

    def keys(self):
        """Returns all keys as a list in current database
        :return: list of keys
        """
        return self.redis.keys()

    def get(self, key, default=None):
        """Return the value at the key if exists else default
        :param key: redis key
        :param default: default value if key not exists
        :return: value at the key or default
        """
        return self.__getitem__(key) if self.__contains__(key) else default

    def pop(self, key, *defaults):
        """If key is in the database, remove it and return its value, else return default.
        :param key: redis key
        :param defaults: default value
        :raise KeyError: when key not exists and default value not given
        """
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

    def setdefault(self, key, default):
        """If key is in the datatbase, return its value.
        If not, insert key with a value of default and return default.
        :param key: redis key
        :param default: default value
        """
        try:
            value = self.redis.__getitem__(key)
        except KeyError:
            self.redis.__setitem__(key, default)
            value = str(default)
        return value

    def getset(self, key, value):
        """Sets the value at key to value and returns the old value at key atomically.
        Warning: currently not support redisugar object rlist/rdict/rset
        :param key: redis key
        :param value: value at key
        :return: old value at key
        """
        return self.redis.getset(key, value)

    def rename(self, src, dst, not_exists=False):
        """Rename key src to dst
        :param src: source key
        :param dst: destination key
        :param not_exists: if True, rename only if destination key not exists
        :return: True/False, rename status
        """
        if not_exists:
            return self.redis.renamenx(src, dst)
        else:
            return self.redis.rename(src, dst)

    def dump(self, key):
        """Return a serialized version of the value stored at the specified key.
        If key does not exist a nil bulk reply is returned.
        :param key: redis key
        :return: bulk string
        """
        return self.redis.dump(key)

    def restore(self, key, ttl, value, replace=False):
        """Create a key using the provided serialized value,
        previously obtained using RediSugar.dump().
        :param key: redis key
        :param ttl: time to live
        :param value: bulk returned from dump()
        :param replace: replace if key exists
        :return: restore status
        """
        return self.redis.restore(key, ttl, value, replace=replace)

    def clear(self):
        """Delete ALL keys in the current database"""
        self.redis.flushdb()

    def save(self, block=False):
        """Tell the Redis server to save its data to disk
        :param block: blocking until the save is complete or not, default False
        """
        if block:
            self.redis.save()
        else:
            self.redis.bgsave()


class rlist(collections.MutableSequence):
    """
    redis list class
    """

    def __init__(self, redisugar, key, iterable=None, dtype=str):
        """Initiate a new redis list object
        :param redisugar: redis.Redis() object
        :param key: redis list key
        :param iterable: an Iterable object to be filled in redis list
        :param dtype: Callable data type specification, dtype(data)
        """
        self.redis = redisugar.redis
        self.key = key
        self.dtype = dtype
        if iterable:
            self.extend(iterable)

    def __len__(self):
        """Return length of rlist
        :return: length
        """
        return self.redis.llen(self.key)

    def __add__(self, other):
        """Concatenate rlist and another list or rlist
        :param other: a list or rlist object
        :return: self + other
        """
        if not isinstance(other, (list, rlist)):
            raise TypeError('can only concatenate list or rlist (not \"{}\") to rlist'.format(get_type(other)))
        if isinstance(other, rlist):
            other = other.copy()
        return self.copy() + other

    def __iadd__(self, other):
        """Update rlist with another list or rlist
        self += other
        :param other: a list or rlist object
        :return: self
        """
        if not isinstance(other, (list, rlist)):
            raise TypeError('can only concatenate list or rlist (not \"{}\") to rlist'.format(get_type(other)))
        if isinstance(other, rlist):
            for item in other:
                self.append(item)
        else:
            self.redis.rpush(self.key, *other)
        return self

    def __radd__(self, other):
        """Concatenate another list or rlist and self
        :param other: a list or rlist object
        :return: other + self
        """
        if not isinstance(other, (list, rlist)):
            raise TypeError('can only concatenate rlist to list or rlist (not \"{}\")'.format(get_type(other)))
        if isinstance(other, rlist):
            other = other.copy()
        return other + self.copy()

    def __mul__(self, other):
        """Multiply rlist with an integer
        :param other: int
        :return: self * other
        """
        if not isinstance(other, int):
            raise TypeError('can\'t multiply sequence by non-int of type ' + get_type(other))
        return self.copy() * other

    def __rmul__(self, other):
        """For calling with int * rlist
        :param other: int
        :return: self * other
        """
        return self.__mul__(other)

    def __imul__(self, other):
        """Update rlist with rlist * int
        :param other: int
        :return: self *= other
        """
        if not isinstance(other, int):
            raise TypeError('can\'t multiply sequence by non-int of type ' + get_type(other))
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
        """Iterator of rlist
        :return: generator object
        """
        i, _len = 0, self.__len__()
        while i < _len:
            yield self[i]
            i += 1

    # def __format__(self, format_spec):
    #     if isinstance(format_spec, unicode):
    #         return unicode(str(self))
    #     else:
    #         return str(self)

    def __repr__(self):
        return '<redisugar.rlist object with key: ' + self.key + '>'

    def __str__(self):
        """Return a summary string of the rlist"""
        _len = self.__len__()
        if _len <= RediSugar.str_summary_limit():
            summary = str(self.copy())
        else:
            with self.redis.pipeline() as pipe:
                pipe.lindex(self.key, 1)
                pipe.lindex(self.key, 2)
                pipe.lindex(self.key, -1)
                head_tail = pipe.execute()
                summary = '[{}, {}, ..., {}]'.format(*head_tail)
        return 'rlist {}, {}, {} elements in total'.format(self.key, summary, _len)

    def _check_index(self, item):
        """Check whether an index is valid
        :param item: index
        :type: int
        """
        if not isinstance(item, int):
            raise TypeError('list indices must be integers, not ' + get_type(item))
        _len = self.__len__()
        if item >= _len or -item < -_len:
            raise IndexError('list index out of range')

    def _read(self, index):
        """Helper function for reading an item from rlist
        :param index: index, int
        :return: item
        """
        return self.redis.lindex(self.key, index)

    def _write(self, index, value):
        """Helper function for writing an item to given index
        :param index: index, int
        :param value: value
        """
        self.redis.lset(self.key, index, value)

    def __getitem__(self, key):
        """For calling with self[key]
        :param key: index
        :type: int, slice
        :return: item or sublist
        """
        if isinstance(key, slice):
            start, stop, step = key.indices(self.__len__())
            with self.redis.pipeline() as pipe:
                for i in range(start, stop, step):
                    self._check_index(i)
                    pipe.lindex(self.key, i)
                result = [self.dtype(x) for x in pipe.execute()]
            return result
        else:
            self._check_index(key)
            return self.dtype(self._read(key))

    def __setitem__(self, key, value, pipeline=None):
        """For assignment calling self[key] = value
        :param key: index
        :type: int, slice
        :param value: item or list
        :param pipeline: an existing redis.pipeline object to perform setting commands on
        """
        if isinstance(key, slice):
            if not isinstance(value, Iterable):
                raise TypeError('can only assign an iterable')
            value = list(value)
            start, stop, step = key.indices(self.__len__())
            if step == 1:
                k_i, k_len = 0, len(value)
                pipe = pipeline if pipeline else self.redis.pipeline()
                with pipe:
                    for i in range(start, stop, step):
                        if k_i < k_len:
                            # self._write(i, value[k_i])
                            pipe.lset(self.key, i, value[k_i])
                            k_i += 1
                        else:
                            break
                    else:
                        if k_len - k_i == 0:
                            pipe.execute()
                            return
                        jump = k_len - k_i
                        # self.extend([self._read(-1)] * jump)
                        pipe.rpush(self.key, *([None] * jump))
                        i = self.__len__() - 1
                        # while i - jump >= stop:
                        while i >= stop:
                            # self._write(i, self._read(i - jump))
                            pipe.lset(self.key, i + jump, self._read(i))
                            i -= 1
                        k_len -= 1
                        # while i >= stop:
                        while i + jump >= stop:
                            # self._write(i, value[k_len])
                            pipe.lset(self.key, i + jump, value[k_len])
                            i -= 1
                            k_len -= 1
                    # pipe.execute()
                    if start + k_i < stop:
                        # del self[start + k_i: stop]
                        self.__delitem__(slice(start + k_i, stop), pipe)
                    else:
                        pipe.execute()
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

    def __delitem__(self, key, pipeline=None):
        """For calling with del self[key]
        :param key: index
        :type: int, slice
        :param pipeline: an existing redis.pipeline object to perform setting commands on
        """
        pipe = pipeline if pipeline else self.redis.pipeline()
        if isinstance(key, slice):
            with pipe:
                start, stop, step = key.indices(self.__len__())
                to_del = set(range(start, stop, step))
                i, counter = start, 0
                while i < self.__len__():
                    if counter == len(to_del):
                        break
                    if i in to_del:
                        counter += 1
                    else:
                        # self._write(i - counter, self._read(i))
                        pipe.lset(self.key, i - counter, self._read(i))
                    i += 1
                while i < self.__len__():
                    # self._write(i - counter, self._read(i))
                    pipe.lset(self.key, i - counter, self._read(i))
                    i += 1
                # self.redis.ltrim(self.key, 0, -counter - 1)
                pipe.ltrim(self.key, 0, -counter - 1)
                pipe.execute()
        else:
            self._check_index(key)
            if key < 0:
                key += self.__len__()
            if key == 0:
                self.redis.lpop(self.key)
            elif key == self.__len__() - 1:
                self.redis.rpop(self.key)
            else:
                with pipe:
                    while key < self.__len__() - 1:
                        # self._write(key, self._read(key + 1))
                        pipe.lset(self.key, key, self._read(key + 1))
                        key += 1
                    # self.redis.rpop(self.key)
                    pipe.rpop(self.key)
                    pipe.execute()

    def __contains__(self, item):
        """For calling with item in rlist
        :param item: item to check
        :return: True/False
        """
        i = 0
        while i < self.__len__():
            if self.dtype(self._read(i)) == item:
                return True
            i += 1
        return False

    def append(self, item):
        """Add one item to the end of the rlist
        :param item: item to be added
        """
        self.redis.rpush(self.key, item)

    def count(self, item):
        """Return number of appearance of given item in rlist
        :param item: item to count
        :return acc: number of appearance of item
        """
        acc, i = 0, 0
        while i < self.__len__():
            if self.dtype(self._read(i)) == item:
                acc += 1
            i += 1
        return acc

    def extend(self, iterable):
        """Extend the rlist with an Iterable object
        :param iterable: an Iterable object
        """
        if not isinstance(iterable, Iterable):
            raise TypeError('\'{0}\' object is not iterable'.format(get_type(iterable)))
        self.redis.rpush(self.key, *list(iterable))

    def index(self, item, start=0, stop=-1):
        """Return index of item in rlist or raise ValueError if not found
        :param item: item to find
        :param start: start index
        :param stop: end index
        :return: item index
        :raise ValueError: when item not in the rlist
        """
        start, stop, _ = slice(start, stop, 1).indices(self.__len__())
        if stop < start:
            raise ValueError('{0} is not in list'.format(item))
        i = start
        while i <= stop:
            if self.dtype(self._read(i)) == item:
                return i
            i += 1
        raise ValueError('{0} is not in list'.format(item))

    def insert(self, index, item):
        """Insert an item into the rlist
        :param index: insert index
        :param item: item to insert
        """
        if index < 0:
            index += self.__len__()
        if index == 0:
            self.redis.lpush(self.key, item)
        elif index >= self.__len__():
            self.redis.rpush(self.key, item)
        else:
            with self.redis.pipeline() as pipe:
                # self.redis.rpush(self.key, self._read(-1))
                pipe.rpush(self.key, self._read(-1))
                # i = self.__len__() - 2
                i = self.__len__() - 1
                while i > index:
                    # self._write(i, self._read(i - 1))
                    pipe.lset(self.key, i, self._read(i - 1))
                    i -= 1
                # self._write(index, item)
                pipe.lset(self.key, index, item)
                pipe.execute()

    def pop(self, pos=-1):
        """Pop one item from the rlist
        :param pos: position to pop
        :return item: item at position
        """
        if self.__len__() == 0:
            raise IndexError('pop from empty list')
        self._check_index(pos)
        item = self.dtype(self._read(pos))
        self.__delitem__(pos)
        return item

    def push(self, item, pos=-1):
        """Push one item to head or tail of the rlist
        :param item: item to push
        :param pos: -1 or 0, default -1
        """
        if pos not in (0, -1):
            raise ValueError('pos can only be 0 or -1 (head or tail)')
        if pos == 0:
            self.redis.lpush(self.key, item)
        elif pos == -1:
            self.redis.rpush(self.key, item)

    def remove(self, item, count=1):
        """Remove item(s) from the rlist
        :param item: item to remove
        :param count: number of items to remove, from left
        :raise ValueError: when item not found
        """
        flag = self.redis.lrem(self.key, item, count)
        if flag == 0:
            raise ValueError('rlist.remove(x): {0} not in rlist'.format(item))

    def reverse(self):
        """Reverse the rlist in place, will create a temp redis object"""
        tmp_key = self.key + str(id(self))
        while self.__len__() != 0:
            tmp = self.redis.rpop(self.key)
            self.redis.rpush(tmp_key, tmp)
        self.redis.rename(tmp_key, self.key)

    def sort(self, reverse=False, alpha=False):
        """Sort the rlist in place
        :param reverse: descending order
        :param alpha:  sorting lexicographically
        """
        self.redis.sort(self.key, alpha=alpha, desc=reverse, store=self.key)

    def copy(self):
        """Return a copy of the rlist into memory
        :return: copied list
        """
        temp = self.redis.lrange(self.key, 0, -1)
        return map(self.dtype, temp)

    def clear(self):
        """Remove all items in the rlist"""
        self.redis.delete(self.key)


class rdict(collections.MutableMapping):
    """
    redis dict class

    Note:
        - builtin dict methods (viewitems(), viewkeys(), viewvalues()) that return view object are not implemented
        - rdict.copy() method is an alias of rdict.items() method but slightly different from dict.copy()
        - several methods are implemented to take advatange of redis-py interfaces

    Warning:
        - key and value only support str type at present, all other type will be converted to str
        - None will be converted to 'None' in redis
    """

    def __init__(self, redisugar, key, *args, **kwargs):
        """Initiate a new redis hash object
        :param redisugar: redis.Redis() object
        :param key: redis hash key
        """
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
        """Helper funciton for update Iterable or Mapping or kwargs into rdict
        :param iterable_or_mapping: Mapping or Iterable object
        :raises ValueError, TypeError
        """
        with self.redis.pipeline() as pipe:
            if iterable_or_mapping:
                if isinstance(iterable_or_mapping, Mapping):
                    for k in iterable_or_mapping:
                        # self._write(k, iterable_or_mapping[k])
                        pipe.hset(self.key, k, iterable_or_mapping[k])
                elif isinstance(iterable_or_mapping, Iterable):
                    for i, each in enumerate(iterable_or_mapping):
                        try:
                            if len(each) != 2:
                                raise ValueError('dictionary update sequence element #{0} has length {1}; 2 is '
                                                 'required'.format(i, len(each)))
                            else:
                                # self._write(each[0], each[1])
                                pipe.hset(self.key, each[0], each[1])
                        except TypeError:
                            raise TypeError('cannot convert dictionary update sequence element #{0} to a '
                                            'sequence'.format(i))
                else:
                    raise TypeError('\'{0}\' object is not iterable'.format(get_type(iterable_or_mapping)))
            if kwargs:
                for k in kwargs:
                    # self._write(k, kwargs[k])
                    pipe.hset(self.key, k, kwargs[k])
            pipe.execute()

    def _raise_not_hashable(self, item):
        """Try to hash an item"""
        hash(item)

    def _check_key_exists(self, key):
        """Raise KeyError if key is not in rdict"""
        if not self.redis.hexists(self.key, key):
            raise KeyError(str(key))

    def _read(self, key):
        """Helper function to read a value from rdict
        :param key: rdict key
        :return: value at the key
        """
        return self.redis.hget(self.key, key)

    def _write(self, key, value):
        """Helper function to write a k-v pair into rdict
        :param key: rdict key
        :param value: value
        """
        self.redis.hset(self.key, key, value)

    def _del(self, key):
        """Helper function to delete a key from rdict
        :param key: rdict key
        """
        self.redis.hdel(self.key, key)

    def __contains__(self, item):
        """Check whether a key is in the rdict
        :param item: rdict key
        :return: True/False
        """
        self._raise_not_hashable(item)
        return self.redis.hexists(self.key, item)

    def __len__(self):
        """Return length of the rdict"""
        return self.redis.hlen(self.key)

    def __getitem__(self, item):
        """For calling with self[key]
        :param item: rdict key
        :return: value at the key
        :raise TypeError: when item is not hashable
        :raise KeyError: when item is not in the rdict
        """
        self._raise_not_hashable(item)
        self._check_key_exists(item)
        return self._read(item)

    def __setitem__(self, key, value):
        """For calling with self[key] = value
        :param key: rdict key
        :param value: value
        :raise TypeError: when item is not hashable
        """
        self._raise_not_hashable(key)
        self._write(key, value)

    def __delitem__(self, key):
        """For calling with del self[key]
        :param key: rdict key
        :raise TypeError: when item is not hashable
        """
        self._raise_not_hashable(key)
        self._check_key_exists(key)
        self._del(key)

    def __iter__(self):
        """Return iterator of rdict keys"""
        return self.iterkeys()

    def __repr__(self):
        return '<redisugar.rdict object with key: ' + self.key + '>'

    def __str__(self):
        """Return a summary string of rdict"""
        _len = self.__len__()
        if _len <= RediSugar.str_summary_limit():
            summary_str = str(self.copy())
        else:
            summary = []
            for pair in self.iteritems():
                if len(summary) == RediSugar.str_summary_limit():
                    break
                summary.append(pair)
            summary_str = '{' + ', '.join(': '.join(pair) for pair in summary) + ' ...}'
        return 'rdict {}, {}, {} elements in total'.format(self.key, summary_str, _len)

    # def __format__(self, format_spec):
    #     if isinstance(format_spec, unicode):
    #         return unicode(str(self))
    #     else:
    #         return str(self)

    def clear(self):
        """Delete all keys in the rdict"""
        self.redis.delete(self.key)

    def copy(self):
        """
        alias of rdict.items()
        """
        return self.items()

    @classmethod
    def fromkeys(cls, redisugar, key, seq, value=None):
        """Build a new rdict from keys
        :param redisugar: redis.Redis() object
        :param key: rdict key
        :param seq: key sequence
        :param value: init value in rdict
        :return: rdict object
        """
        rd = cls(redisugar, key)
        with rd.redis.pipeline() as pipe:
            for each in seq:
                # rd[each] = value
                rd._raise_not_hashable(each)
                pipe.hset(rd.key, each, value)
            pipe.execute()
        return rd

    def get(self, key, default=None):
        """Get value at the key or default if key not found
        :param key: rdict key
        :param default: default value if key not found
        :return: value at the key or default
        """
        return self._read(key) if self.__contains__(key) else default

    def keys(self):
        """Return all keys in the rdict"""
        return self.redis.hkeys(self.key)

    def values(self):
        """Return all values in the rdict"""
        return self.redis.hvals(self.key)

    def items(self):
        """Return all k-v pair in the rdict"""
        return self.redis.hgetall(self.key)

    def iterkeys(self):
        """Return an iterator of keys"""
        for each in self.redis.hscan_iter(self.key):
            yield each[0]

    def itervalues(self):
        """Return an iterator of values"""
        for each in self.redis.hscan_iter(self.key):
            yield each[1]

    def iteritems(self):
        """Return an iterator of k-v pairs"""
        for each in self.redis.hscan_iter(self.key):
            yield each

    def pop(self, key, *defaults):
        """Return value at the key and delete the key
        :param key: rdict key
        :param defaults: default value if key not found
        :return value: value at the key or default
        """
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
        """Pop a random k-v pair"""
        for key in self.iterkeys():
            break
        else:
            raise KeyError('popitem(): dictionary is empty')
        value = self._read(key)
        self._del(key)
        return key, value

    def setdefault(self, key, value=None):
        """Return value at the key or set value to the key if key not found
        :param key: rdict key
        :param value: default value if key not found
        :return: value at the ket or value
        """
        if self.__contains__(key):
            return self._read(key)
        else:
            self._write(key, value)
            return str(value)

    def update(*args, **kwds):
        """Update rdict with sequence and keyword parameters
        :param args: expect one argument
        :param kwds: k-v pairs
        """
        self = args[0]
        args = args[1:]
        len_args = len(args)
        if len_args == 1:
            iterable_or_mapping = args[0]
        elif len_args > 1:
            raise TypeError('update expected at most 1 (non-keyword) argument, got {0}'.format(len_args))
        else:
            iterable_or_mapping = None
        self._update(iterable_or_mapping, **kwds)

    def multi_get(self, keys, *args):
        """Get multiple values in order of keys and args
        :param keys: a list of rdict keys
        :type: list
        :param args: rdict keys, will be appended to keys
        :return: list of value at given keys and args
        """
        return self.redis.hmget(self.key, keys, *args)

    def multi_set(self, *args, **kwargs):
        """Set multiple k-v pairs
        :param mapping: k-v pairs
        """
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise TypeError('multi_set requires kwargs or a single dict arg')
            kwargs.update(args[0])
        self.redis.hmset(self.key, kwargs)

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


class rset(collections.MutableSet):
    """
    redis set class

    Warning:
        - only support str type at present, all other types will be converted to str by redis-py

    TODO:
        - set comparison methods
    """

    def __init__(self, redisugar, key, iterable=None):
        self.redis = redisugar.redis
        self.key = key
        if iterable:
            for item in iterable:
                self._raise_not_hashable(item)
                self._write(item)

    def _raise_not_hashable(self, value):
        """Check wether value is hashable
        :raise TypeError: when value is not hashable
        """
        hash(value)

    def _write(self, *values):
        """Write multiple values into redis."""
        if not values:
            return
        self.redis.sadd(self.key, *values)

    def _delete(self, *values):
        """Delete multiple values from redis."""
        if not values:
            return
        self.redis.srem(self.key, *values)

    @classmethod
    def _make_sets(cls, others):
        """Check input parameters and set(parameter) if it is not set/fronzenset/rset
        :param others: list of all other Iterable objects
        :return: list of set/fronzenset/rset
        :raise: TypeError: when parameter is not iterable
        """
        others = [other if isinstance(other, (set, frozenset, rset)) else set(other) for other in others]
        return others

    def _cmp_length(self, other):
        """Compare self and other set by length
        :param other: another set/fronzenset
        :return small, large: small set and large set by length
        """
        if len(other) > self.__len__():
            small, large = self, other
        else:
            small, large = other, self
        return small, large

    def __len__(self):
        """Return length of the rset."""
        return self.redis.scard(self.key)

    def __contains__(self, value):
        """Test value for membership in rset."""
        return self.redis.sismember(self.key, value)

    def __repr__(self):
        return '<redisugar.rset object with key: ' + self.key + '>'

    def __str__(self):
        """Return a summary string of rset"""
        _len = self.__len__()
        if _len <= RediSugar.str_summary_limit():
            summary_str = str(self.copy())
        else:
            summary = []
            for item in self.__iter__():
                if len(summary) == RediSugar.str_summary_limit():
                    break
                summary.append(item)
            summary_str = 'rset([{}, ...])'.format(', '.join(summary))
        return 'rset {}, {}, {} elements in total'.format(self.key, summary_str, _len)

    def __or__(self, other):
        """Return a new set with elements from the rset and the other.
        :param other: another set-like object
        :return: rset | other
        """
        if isinstance(other, rset):
            return self.redis.sunion(self.key, other.key)
        elif isinstance(other, (set, frozenset)):
            return self.union(other)
        else:
            raise TypeError('unsupported operand type(s) for |: \'rset\' and \'{}\''.format(get_type(other)))

    def __ior__(self, other):
        """Update the rset, adding elements from the other.
        self |= other
        :param other: another set-like object
        :return: self
        """
        if isinstance(other, rset):
            self.redis.sunionstore(self.key, self.key, other.key)
        elif isinstance(other, (set, frozenset)):
            self._write(*other)
        else:
            raise TypeError('unsupported operand type(s) for |=: \'rset\' and \'{}\''.format(get_type(other)))
        return self

    def __ror__(self, other):
        """For builtin types that called with other | rset.
        Call self.__or__ according to Commutative Law
        :param other: another set-like object
        :return: other | rset
        """
        try:
            return self.__or__(other)
        except TypeError:
            raise TypeError('unsupported operand type(s) for |: \'{}\' and \'rset\''.format(get_type(other)))

    def __and__(self, other):
        """Return a new set with elements common to the rset and the other.
        :param other: another set-like object
        :return: rset & other
        """
        if isinstance(other, rset):
            return self.redis.sinter(self.key, other.key)
        elif isinstance(other, (set, frozenset)):
            small, large = self._cmp_length(other)
            inter = set(value for value in small if value in large)
            return inter
        else:
            raise TypeError('unsupported operand type(s) for &: \'rset\' and \'{}\''.format(get_type(other)))

    def __iand__(self, other):
        """Update the rset, keeping only elements found in it and the other.
        self &= other
        :param other: another set-like object
        :return: self
        """
        if isinstance(other, rset):
            self.redis.sinterstore(self.key, self.key, other.key)
        elif isinstance(other, (set, frozenset)):
            diff = self.__sub__(other)
            self._delete(*diff)
        else:
            raise TypeError('unsupported operand type(s) for &=: \'rset\' and \'{}\''.format(get_type(other)))
        return self

    def __rand__(self, other):
        """For builtin types that called with other & rset.
        Call self.__and__ according to Commutative Law
        :param other: another set-like object
        :return: other & rset
        """
        try:
            return self.__and__(other)
        except TypeError:
            raise TypeError('unsupported operand type(s) for &: \'{}\' and \'rset\''.format(get_type(other)))

    def __sub__(self, other):
        """Return a new set with elements in the rset that are not in the other.
        :param other: another set-list object
        :return: rset - other
        """
        if isinstance(other, rset):
            return self.redis.sdiff(self.key, other.key)
        elif isinstance(other, (set, frozenset)):
            diff = set(value for value in self if value not in other)
            return diff
        else:
            raise TypeError('unsupported operand type(s) for -: \'rset\' and \'{}\''.format(get_type(other)))

    def __isub__(self, other):
        """Update the rset, keeping only elements found in either set, but not in both.
        self -= other
        :param other: another set-like object
        :return: self
        """
        if isinstance(other, rset):
            self.redis.sdiffstore(self.key, self.key, other.key)
        elif isinstance(other, (set, frozenset)):
            inter = self.__and__(other)
            self._delete(*inter)
        else:
            raise TypeError('unsupported operand type(s) for -=: \'rset\' and \'{}\''.format(get_type(other)))
        return self

    def __rsub__(self, other):
        """For builtin types that called with other - rset
        :param other: another set-like object
        :return: other - rset
        """
        if isinstance(other, rset):
            other.__sub__(self)
        elif isinstance(other, (set, frozenset)):
            inter = self.__and__(other)
            return other - inter
        else:
            raise TypeError('unsupported operand type(s) for -: \'{}\' and \'rset\''.format(get_type(other)))

    def __xor__(self, other):
        """Return a new set with elements in either the rset or other but not both.
        :param other: another set-like object
        :return: self ^ other
        """
        try:
            return self.__or__(other) - self.__and__(other)
        except TypeError:
            raise TypeError('unsupported operand type(s) for ^: \'rset\' and \'{}\''.format(get_type(other)))

    def __ixor__(self, other):
        """Update self to self ^ other
        :param other: another set-like object
        :return: self
        """
        try:
            to_del = self.__and__(other)
            to_add = other - self
            self._delete(*to_del)
            self._write(*to_add)
            return self
        except TypeError:
            raise TypeError('unsupported operand type(s) for ^=: \'rset\' and \'{}\''.format(get_type(other)))

    def __rxor__(self, other):
        """For builtin types that called with other ^ rset
        :param other: another set-like object
        :return: rset ^ other
        """
        try:
            return self.__xor__(other)
        except TypeError:
            raise TypeError('unsupported operand type(s) for ^: \'{}\' and \'rset\''.format(get_type(other)))

    def __iter__(self):
        """Return a generator object of rset"""
        return self.redis.sscan_iter(self.key)

    def copy(self):
        """Copy the rset into memory.
        :return: shallow copy of rset
        :type: set
        """
        return self.redis.smembers(self.key)

    def add(self, value):
        """Add element elem to the rset."""
        self._raise_not_hashable(value)
        self._write(value)

    def discard(self, value):
        """Remove element elem from the rset if it is present.
        :param value: element to remove
        """
        self._raise_not_hashable(value)
        self._delete(value)

    def remove(self, value):
        """Remove element elem from the rset.
        :param value: element to remove
        :raise KeyError: when value not found
        """
        self._raise_not_hashable(value)
        if not self.__contains__(value):
            raise KeyError(str(value))
        else:
            self._delete(value)

    def pop(self):
        """Remove and return an arbitrary element from the rset.
        :return value: random element
        :raise KeyError: when rset is empty
        """
        if self.__len__() == 0:
            raise KeyError('pop from an empty rset')
        value = self.redis.spop(self.key)
        return value

    def clear(self):
        """Remove all elements from the rset."""
        self.redis.delete(self.key)

    def union(self, *others):
        """Return a new set with elements from the rset and all others.
        :param others: list of all other Iterable object
        :return union_set: union of rset and all others
        """
        union_set = self.copy()
        union_set = union_set.union(*others)
        return union_set

    def update(self, *others):
        """Update the rset, adding elements from all others.
        self |= other | ...
        :param others: list if all other Iterable object
        """
        others = self._make_sets(others)
        for other in others:
            self.__ior__(other)

    def intersection(self, *others):
        """Return a new set with elements common to the rset and all others.
        :param others: list of all other Iterable object
        :return intersection_set: intersection of rset and all others
        """
        others = self._make_sets(others)
        others.append(self)
        min_set = min(others, key=lambda x: len(x))
        intersection_set = set()
        for item in min_set:
            if all(item in other for other in others):
                intersection_set.add(item)
        return intersection_set

    def intersection_update(self, *others):
        """Update the rset, keeping only elements found in it and all others.
        self &= other & ...
        :param others: list if all other Iterable object
        """
        others = self._make_sets(others)
        for other in others:
            self.__iand__(other)

    def difference(self, *others):
        """Return a new set with elements in the rset that are not in the others.
        :param others: list of all other Iterable object
        :return difference_set: difference of rset from all others
        """
        others = self._make_sets(others)
        difference_set = set()
        for item in self.__iter__():
            if all(item not in other for other in others):
                difference_set.add(item)
        return difference_set

    def difference_update(self, *others):
        """Update the rset, removing elements found in others.
        self -= other | ...
        :param others: list if all other Iterable object
        """
        others = self._make_sets(others)
        for other in others:
            self.__isub__(other)

    def symmetric_difference(self, other):
        """Return a new set with elements in either the rset or other but not both.
        :param other: another Iterable object
        :return: self ^ other
        :raise: TypeError: when other is not iterable
        """
        if not isinstance(other, (set, frozenset, rset)):
            other = set(other)
        return self.__xor__(other)

    def symmetric_difference_update(self, other):
        """Update the rset, keeping only elements found in either set, but not in both.
        self ^= other
        :param other: another Iterable object
        :raise: TypeError: when other is not iterable
        """
        if not isinstance(other, (set, frozenset, rset)):
            other = set(other)
        return self.__ixor__(other)

    def isdisjoint(self, other):
        """
        Return True if the set has no elements in common with other.
        Sets are disjoint if and only if their intersection is the empty set.
        :param other: another Iterable object
        :return: True/False
        :raise: TypeError: when other is not iterable
        """
        if not isinstance(other, (set, frozenset, rset)):
            other = set(other)
        small, large = self._cmp_length(other)
        for item in small:
            if item in large:
                return False
        return True

    def issubset(self, other):
        """Test whether every element in the set is in other.
        :param other: another Iterable object
        :return: True/False
        :raise: TypeError: when other is not iterable
        """
        if not isinstance(other, (set, frozenset, rset)):
            other = set(other)
        for item in self.__iter__():
            if item not in other:
                return False
        return True

    def issuperset(self, other):
        """Test whether every element in other is in the set.
        :param other: another Iterable object
        :return: True/False
        :raise: TypeError: when other is not iterable
        """
        if not isinstance(other, (set, frozenset, rset)):
            other = set(other)
        for item in other:
            if not self.__contains__(item):
                return False
        return True


class rstr(object):
    """
    redis string class

    Warning:
        - Since python str object is immutable, it makes no sense to implement all python str interface for redis
        string. This class only supports special redis string commands like INCR.
        - You will need this object, only if you want update a redis string without copying it into memory. If you
        want to use it like python str, just copy it to a str object, play, then update it back into redis.
        - RediSugar[key] only return as python str object, you must explicitly create a rstr object.
        - __iadd__ interface is implemented to take advantage of APPEND command in redis
    """
    def __init__(self, redisugar, key, value=''):
        self.redis = redisugar.redis
        self.key = key
        if value:
            self.redis.set(key, value)

    @classmethod
    def multi_set(cls, redisugar, *args, **kwargs):
        """Alias of redis.mset, set multiple k-v pair, *assume all values are string*
        :param redisugar: redisugar.RediSugar object
        :param args: expect a single dict
        :param kwargs: kwargs will be updated into dict
        """
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise TypeError('multi_set requires kwargs or a single dict arg')
            args[0].update(kwargs)
            kwargs = args[0]
        redisugar.redis.mset(kwargs)

    @classmethod
    def multi_set_not_exist(cls, redisugar, *args, **kwargs):
        """Alias of redis.msetnx, set multiple k-v pair only if all keys are not present, *assume all values are string*
        :param redisugar: redisugar.RediSugar object
        :param args: expect a single dict
        :param kwargs: kwargs will be updated into dict
        :raise ValueError: when at least one key is already in redis
        """
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise TypeError('multi_set_not_exists requires kwargs or a single dict arg')
            args[0].update(kwargs)
            kwargs = args[0]
        status = redisugar.redis.msetnx(kwargs)
        if not status:
            raise ValueError('at least one key is already in redis')

    @classmethod
    def multi_get(cls, redisugar, keys, *args):
        """Alias of redis.mget, *assume all values are string*
        :param redisugar: redisugar.RediSugar object
        :param keys: list of keys
        :param args: keys will be appended to keys
        :return: list of values
        """
        return redisugar.redis.mget(keys, *args)

    def __repr__(self):
        return '<redisugar.rstr object with key: ' + self.key + '>'

    def __str__(self):
        return self.redis.get(self.key)

    def __len__(self):
        return self.redis.strlen(self.key)

    def __iadd__(self, other):
        """Append other to self
        :param other: str or rstr
        :return: self
        """
        self.redis.append(self.key, other)
        return self

    def _check_index(self, item):
        """Check whether an index is valid
        :param item: index
        :type: int
        """
        if not isinstance(item, int):
            raise TypeError('string indices must be integers, not ' + get_type(item))
        _len = self.__len__()
        if item >= _len or -item < -_len:
            raise IndexError('string index out of range')

    def __getitem__(self, key):
        """For calling with self[key]
        :param key: index
        :type: int, slice
        :return: substring
        """
        if isinstance(key, slice):
            start, stop, step = key.indices(self.__len__())
            self._check_index(start)
            self._check_index(stop - 1)
            if step == 1:
                # getrange(key, start, stop) will return start, stop inclusively
                result_str = self.redis.getrange(self.key, start, stop - 1)
            else:
                with self.redis.pipeline() as pipe:
                    for i in range(start, stop, step):
                        pipe.getrange(self.key, i, i)
                    result = pipe.execute()
                result_str = ''.join(result)
            return result_str
        else:
            self._check_index(key)
            return self.redis.getrange(self.key, key, key)

    def set(self, value):
        """Set a new value to the key
        :param value: new value
        """
        self.redis.set(self.key, value)

    def decrease(self, decrement=1):
        """Decrease the integer value at the key by decrement
        :param decrement: decrease by number, default 1
        """
        if isinstance(decrement, int):
            self.redis.decr(self.key, decrement)
        else:
            raise TypeError('can only decrease by int')

    def increase(self, increment=1):
        """Increase the integer value at the key by decrement
        :param increment: increase by number, default 1
        """
        if isinstance(increment, int):
            self.redis.incr(self.key, increment)
        elif isinstance(increment, float):
            self.redis.incrbyfloat(self.key, increment)
        else:
            raise TypeError('can only increase by int or float')

    def set_range(self, offset, value):
        """ Quote from redis-py document [http://redis-py.readthedocs.io/en/latest/]:
        "Overwrite bytes in the value of name starting at offset with value. If offset plus the length of value
        exceeds the length of the original value, the new value will be larger than before. If offset exceeds the
        length of the original value, null bytes will be used to pad between the end of the previous value and the
        start of what’s being injected."
        null byte is \x00
        :return: length of the new string
        """
        return self.redis.setrange(self.key, offset, value)


class sorted_set(collections.MutableSet, collections.MutableMapping):
    """ Sorted Set data structure internally supported by redis, this class simply wrap redis-py z* APIs.
    Note:
        - Consider to implement set-like interfaces in future
        - Sorted Set inherit collections.MutableMapping, therefore supporting dict-like interfaces
    Warning:
        - *_lex suffix methods are not implemented at this time
    """

    def __init__(self, redisugar, key, iterable=None):
        self.redis = redisugar.redis
        self.key = key
        if iterable:
            self.add(iterable)

    @classmethod
    def intersection_store(cls, redisugar, destination, keys, weights=None, aggregate=None, overwrite=True):
        """Intersect multiple sorted sets specified by keys into destination
        :param redisugar: RediSugar object
        :param destination: destination key
        :param keys: sorted set keys
        :param weights: weights for each keys
        :param aggregate: how scores are aggregated over sets, default 'SUM', choices are 'SUM', 'MIN', 'MAX'
        :param overwrite: overwrite destination key if exists, default True
        :return: destination sorted set length
        """
        if aggregate and aggregate not in ('SUM', 'MIN', 'MAX'):
            raise ValueError('unsupport aggregate method: ' + str(aggregate))
        if not overwrite and destination in redisugar:
            raise ValueError('destination already exists: ' + destination)
        if weights:
            if len(keys) != len(weights):
                raise ValueError('weights must have same length with keys')
            keys = {x: y for x, y in zip(keys, weights)}
        return redisugar.redis.zinterstore(destination, keys, aggregate=aggregate)

    @classmethod
    def union_store(cls, redisugar, destination, keys, weights=None, aggregate=None, overwrite=True):
        """Union multiple sorted sets specified by keys into destination
        :param redisugar: RediSugar object
        :param destination: destination key
        :param keys: sorted set keys
        :param weights: weights for each keys
        :param aggregate: how scores are aggregated over sets, default 'SUM', choices are 'SUM', 'MIN', 'MAX'
        :param overwrite: overwrite destination key if exists, default True
        :return: destination sorted set length
        """
        if aggregate and aggregate not in ('SUM', 'MIN', 'MAX'):
            raise ValueError('unsupport aggregate method: ' + str(aggregate))
        if not overwrite and destination in redisugar:
            raise ValueError('destination already exists: ' + destination)
        if weights:
            if len(keys) != len(weights):
                raise ValueError('weights must have same length with keys')
            keys = {x: y for x, y in zip(keys, weights)}
        return redisugar.redis.zunionstore(destination, keys, aggregate=aggregate)

    def _make_writable(self, *args, **kwargs):
        if len(args) == 0:
            return kwargs
        if len(args) == 1 and isinstance(args[0], Iterable):
            iterable = args[0]
        else:
            iterable = args
        try:
            pair_dict = dict(iterable)
        except TypeError as type_e:
            if len(args) % 2 != 0:
                raise ValueError('{} and cannot build dict from odd length args'.format(type_e.message))
            else:
                pair_dict = {iterable[i]: iterable[i + 1] for i in xrange(0, len(iterable), 2)}
        pair_dict.update(kwargs)
        return pair_dict

    def _write(self, pair_dict):
        """Write a dict into redis sorted set
        :param pair_dict: {key: score}
        :type pair_dict: dict
        """
        if not pair_dict:
            return
        self.redis.zadd(self.key, **pair_dict)

    def _delete(self, *values):
        """Delete values from sorted set"""
        if not values:
            return
        self.redis.zrem(self.key, *values)

    def __iter__(self):
        """Return an iterator over all elements in the sorted set"""
        return self.redis.zscan_iter(self.key)

    def __len__(self):
        """Return length of the sorted set"""
        return self.redis.zcard(self.key)

    def __contains__(self, x):
        """Check whether a element is in the sorted set, in a simple way"""
        return self.redis.zscore(self.key, x) is not None

    def __repr__(self):
        return '<redisugar.sorted_set object with key: ' + self.key + '>'

    def __str__(self):
        """Return a summary string of sorted set"""
        _len = self.__len__()
        if _len <= RediSugar.str_summary_limit():
            summary_str = 'set([{}, ...])'.format(', '.join(str(x) for x in self.copy()))
        else:
            summary = []
            for item in self.__iter__():
                if len(summary) == RediSugar.str_summary_limit():
                    break
                summary.append(item)
            summary_str = 'set([{}, ...])'.format(', '.join(str(x) for x in summary))
        return 'sorted_set {}, {}, {} elements in total'.format(self.key, summary_str, _len)

    def copy(self):
        """Return a copy of sorted_set as a list if (value, score) pairs"""
        return list(self.__iter__())

    def clear(self):
        """Remove all elements from the sorted set"""
        self.redis.delete(self.key)

    def discard(self, value):
        """Delete an element by key"""
        raise_not_hashable(value)
        self._delete(value)

    def remove(self, value):
        """Delete an element by key, raise KeyError if not found"""
        raise_not_hashable(value)
        if not self.__contains__(value):
            raise KeyError(value)
        else:
            self._delete(value)

    def add(self, *values, **kwargs):
        """Add element pairs into sorted set, pairs will be built as dict(values).update(kwargs) in an acceptable way.
        :param values: can contain one iterable or be an iterable itself that can be built into a dict (or even length)
        :param kwargs: element pairs as a dict
        """
        pair_dict = self._make_writable(*values, **kwargs)
        [raise_not_hashable(x) for x in pair_dict]
        self._write(pair_dict)

    def __setitem__(self, key, value):
        """Provides python dict-like set syntax sugar
        :param key: sorted set value
        :param value: score of the key
        """
        if isinstance(key, (str, unicode)):
            self.redis.zadd(self.key, key, value)
        else:
            raise TypeError('set syntax expected str/unicode key, got {}'.format(get_type(key)))

    def score(self, value, *more):
        """Return the scores of given values
        :param value: element value
        :param more: if more values are given, return a list of scores
        :return: single score or a list of scores
        """
        if len(more) > 0:
            with self.redis.pipeline() as pipe:
                pipe.zscore(self.key, value)
                for item in more:
                    pipe.zscore(self.key, item)
                result = pipe.execute()
            return result
        else:
            return self.redis.zscore(self.key, value)

    def incr_by(self, value, increment):
        """Increase score of value by increment"""
        self.redis.zincrby(self.key, value, increment)

    def rank(self, value, reverse=False):
        """Returns a 0-based value indicating the rank of value in sorted_set
        :param value: sorted set element
        :param reverse: if set True, scores ordered from high to low
        """
        if not reverse:
            return self.redis.zrank(self.key, value)
        else:
            return self.redis.zrevrank(self.key, value)

    def count(self, _min, _max):
        """Return number of elements within min max inclusively,
        _min, _max should be int or float
        """
        return self.redis.zcount(self.key, _min, _max)

    def count_by_lex(self, _min, _max):
        raise NotImplementedError

    def range(self, start, stop, reverse=False, withscores=False, score_cast_func=None):
        """Return range with rank as index, behaves like python slice operator.
        Provides slice syntax sugar sorted_set[1:5:1], only for range().
        :param start: start index, int, inclusive
        :param stop: stop index, int, exclusive
        :param reverse: if set True, scores ordered from high to low
        :param withscores: if set True, return elements with score (value, score)
        :param score_cast_func: callable object to cast the score return value.
        Note that, score is a string in redis and this method will convert it to float before pass into func.
        So this func will be called, score_cast_func(float(score)), if you really want score as a string, convert it
        explicitly in yout func.
        """
        # let range behaves like python slice
        stop -= 1
        score_cast_func = lambda x: score_cast_func(float(x)) if score_cast_func else None
        return self.redis.zrange(
            self.key,
            start,
            stop,
            desc=reverse,
            withscores=withscores,
            score_cast_func=score_cast_func
        )

    def __getitem__(self, key):
        """Provides slice syntax sugar for rank index range access"""
        if isinstance(key, int):
            _list = self.redis.zrange(self.key, key, key)
            return _list and _list[0]
        else:
            start, stop, step = key.indices(self.__len__())
            if step == 1:
                stop -= 1
                _list = self.redis.zrange(self.key, start, stop)
            else:
                with self.redis.pipeline() as pipe:
                    for i in range(start, stop, step):
                        pipe.zrange(self.key, i, i)
                    _list = [x[0] for x in pipe.execute() if x]
            return _list

    def range_by_lex(self, _min, _max, reverse=False):
        raise NotImplementedError

    def range_by_score(self, _min, _max, start=None, num=None, reverse=False, withscores=False, score_cast_func=None):
        """Return a range of values with scores between min and max, inclusively.
        :param _min: min score, inclusively
        :param _max: max score, inclusively
        :param start: slice start, if given, will return slice of the range
        :param num: slice length, as above
        :param reverse: if set True, scores ordered from high to low
        :param withscores: if set True, return elements with score (value, score)
        :param score_cast_func: as range
        :return: list of elements
        """
        score_cast_func = lambda x: score_cast_func(float(x)) if score_cast_func else None
        if not reverse:
            range_func = self.redis.zrangebyscore
        else:
            range_func = self.redis.zrevrangebyscore
        return range_func(
            self.key,
            _min,
            _max,
            start=start,
            num=num,
            withscores=withscores,
            score_cast_func=score_cast_func
        )

    def remove_range(self, _min, _max):
        """Remove elements with rank as index, behaves like python slice opration
        :param _min: start rank, inclusively
        :param _max: end rank, exclusively
        """
        _max -= 1
        return self.redis.zremrangebyrank(_min, _max)

    def __delitem__(self, key):
        """Provides slice syntax sugar for rank index range delete"""
        if isinstance(key, int):
            return self.redis.zremrangebyrank(key, key)
        else:
            start, stop, step = key.indices(self.__len__())
            if step == 1:
                stop -= 1
                return self.redis.zremrangebyrank(start, stop)
            else:
                with self.redis.pipeline() as pipe:
                    for i in range(start, stop, step):
                        pipe.zremrangebyrank(i, i)
                    return sum(pipe.execute())

    def remove_range_by_lex(self, _min, _max):
        raise NotImplementedError

    def remove_range_by_score(self, _min, _max):
        """Remove all elements with scores between min and max, inclusively
        :param _min: min score, inclusively
        :param _max: max score, inclusively
        :return: number of elements removed
        """
        return self.redis.zremrangebyscore(_min, _max)