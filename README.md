redisugar
==========
[![Build Status](https://travis-ci.org/pengmeng/redisugar.svg?branch=master)](https://travis-ci.org/pengmeng/redisugar)  
Pythonic redis interface based on redis-py  
Main purpose of this project is provding pythonic redis (data structure) interfaces that are in consistent with
python builtin data structures. So you can use any supported redis data structures just like using builtin python
library.  
Currently supporting redis data structures are:

 - list
 - hash
 - set
 - string
 - sorted set

For full [redis documentation](http://redis.io/documentation).

Getting Started
---------------
### database level interface
```
>>> from redisugar import RediSugar
>>> sugar = RediSugar.get_sugar(db=1)
>>> sugar['a'] = '1'
>>> sugar['a']
'1'
>>> del sugar['a']
>>> 'a' in sugar
False
>>> sugar['a']
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "redisugar/sugar.py", line 57, in __getitem__
    raise KeyError(str(key))
KeyError: 'a'
```

### redis list interface
```
>>> from redisugar import rlist
>>> l = rlist(sugar, 'mylist', [1, 2, 3])
>>> len(l)
3
>>> l.append(4)
>>> l.copy()
['1', '2', '3', '4']
>>> l[2:-1]
['3']
>>> del l[2]
>>> l.copy()
['1', '2', '4']
>>> l + ['5']
['1', '2', '4', '5']
>>> l += ['10']
>>> l.copy()
['1', '2', '4', '10']
```

### redis dict interface
```
>>> from redisugar import rdict
>>> d = rdict(sugar, 'mydict', a=1, b=2, c=3)
>>> d.copy()
{'a': '1', 'c': '3', 'b': '2'}
>>> 'd' in d
False
>>> d.pop('c')
'3'
>>> len(d)
2
>>> list(d)
['a', 'b']
>>> d.clear()
>>> len(d)
0
>>> rdict.fromkeys(sugar, 'mydict', [1, 2, 3], 'none').copy()
{'1': 'none', '3': 'none', '2': 'none'}
```

### redis rset interface
```
>>> from redisugar import rset
>>> s = rset(sugar, 'myset', [1, 2, 3, 4])
>>> '5' in s
False
>>> '2' in s
True
>>> s & {2, 3}
set([2, 3])
>>> {3, 4, 5} - s
set([5])
>>> s.remove(4)
>>> len(s)
3
>>> s.remove(5)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "redisugar/sugar.py", line 893, in remove
    raise KeyError(str(value))
KeyError: '5'
>>> s &= {'1'} # must use str type in set
>>> s.copy()
set(['1'])
```

### redis rstr interface
```
>>> from redisugar import RediSugar, rstr
>>> s = rstr(sugar, 'mystring', 'abc123')
>>> str(s)
'abc123'
>>> s += 'def'
>>> str(s)
'abc123def'
>>> s[3:6]
'123'
>>> s.set(s[3:6])
>>> str(s)
'123'
>>> s.decrease(23)
>>> str(s)
'100'
>>> s.set_range(1, '11')
3
>>> str(s)
'111'
```

### redis sorted_set / rzset interface
```
from redisugar import RediSugar, rzset
>>> rzset
<class 'redisugar.sugar.sorted_set'>
>>> data = [('a', 1), ('b', 2), ('c', 3), ('d', 4)]
>>> zs = rzset(sugar, 'myrzset', data)
>>> print(zs)
sorted_set myrzset, set([('a', 1.0), ('b', 2.0), ('c', 3.0), ('d', 4.0)]), 4 elements in total
>>> 'a' in zs
True
>>> zs['a'] # get score by value
1.0
>>> zs[2] # get value by rank
'c'
>>> zs[:2] # get range by rank
['a', 'b']
>>> del zs[1:3] # remove range by rank
>>> print(zs)
sorted_set myrzset, set([('a', 1.0), ('d', 4.0)]), 2 elements in total
```

### get inner redis object to obtain all redis-py commands
```
>>> r = sugar.redis
>>> r
Redis<ConnectionPool<Connection<host=localhost,port=6379,db=1>>>
>>> dir(r)
[...'append', 'bgrewriteaof', 'bgsave', 'bitcount', 'bitop', 'bitpos',
'blpop', 'brpop', 'brpoplpush', 'client_getname', 'client_kill',
'client_list', 'client_setname', 'config_get', 'config_resetstat',
'config_rewrite', 'config_set', 'connection_pool', 'dbsize',
'debug_object', 'decr', 'delete', 'dump', 'echo', 'eval', ...]
```


Warning & Notes
---------------
### rlist
 - supporting data type by dtype keyword parameter, only for test as present

### rdict
 - key and value only support str type at present, all other type will be converted to str
 - None will be converted to 'None' in redis
 - builtin dict methods (viewitems(), viewkeys(), viewvalues()) that return view object are not implemented
 - rdict.copy() method is an alias of rdict.items() method but slightly different from dict.copy()

### rset
 - only support str type at present, all other type will be converted to str

### rstr
 - Since python str object is immutable, it makes no sense to implement all python str interface for redis
string. This class only supports special redis string commands like INCR.
 - You will need this object, only if you want update a redis string without copying it into memory. If you
want to use it like python str, just copy it to a str object, play, then update it back into redis.
 - RediSugar\[key\] only return as python str object, you must explicitly create a rstr object.

### rzset / sorted_set
 - Consider to implement set-like interfaces in future
 - Sorted Set also inherit collections.MutableMapping, therefore supporting dict-like interfaces
 - *_lex suffix methods are not implemented at this time


TODO
----
 - add comparison methods for all data structures
 - implement set-like interfaces for rzset class


Acknowledgement
---------------
 - [redis-py](https://github.com/andymccurdy/redis-py) by Andy McCurdy
