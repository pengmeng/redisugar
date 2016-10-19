redisugar
==========
[![Build Status](https://travis-ci.com/pengmeng/redisugar.svg?token=ns6e33dpnP1KMQ4NmfpJ&branch=master)](https://travis-ci.com/pengmeng/redisugar)  
Pythonic redis interface based on redis-py  
Main purpose of this project is provding pythonic redis (data structure) interfaces that are in consistent with python builtin data structures.  
So you can use any supported redis data structures just like using builtin python library.  
Currently support redis data structures are:  

 - list
 - hash
 - set
 
For full [redis documentation](http://redis.io/documentation).

Getting Started
---------------
### database level interface
```
>>> from redisugar import RediSugar
>>> sugar = RediSugar.getSugar(db=1)
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


TODO
----
 - add comparison methods for all data structures
 - use pipeline to optimize some operations


Acknowledgement
---------------
 - [redis-py](https://github.com/andymccurdy/redis-py) by Andy McCurdy  