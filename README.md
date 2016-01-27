[![Build Status](https://travis-ci.org/internap/netman.svg?branch=master)](https://travis-ci.org/internap/redlock-fifo)
[![PyPI version](https://badge.fury.io/py/redlock-fifo.svg)](http://badge.fury.io/py/redlock-fifo)

Redlock-FIFO
============

Redlock-FIFO adds true FIFO ordering on top of [Redlock-py](https://github.com/SPSCommerce/redlock-py).
Similar to other implementations of [Redlock](http://redis.io/topics/distlock), extend support was also added.


Why would I use this ?
----------------------

If you need a true FIFO in a lightweight distributed locking manager.

To prevent [starvation](https://en.wikipedia.org/wiki/Starvation_%28computer_science%29), we add a simple FIFO queue 
implemented on top of redlock to manage access to the requested lock. This queue allows us to guarantee FIFO access to a resource.


Installation
------------

    python setup.py install


Usage
-----

    # create a lock manager
    lockmanager = FIFORedlock([{“host”: “localhost”, “port”: 6379, “db”: 0}]) 
    
    # Acquire a lock for 1000ms
    my_lock = lockmanager.lock("my_resource_name", 1000)
    
    # To extend a lock for another 1000ms
    lockmanager.extend(my_lock, 1000)
    
    # To release a lock
    lockmanager.unlock(my_lock)


Running Tests
-------------

    tox -r

These tests make sure that the lock algorithm is reliable. Test suite is run against python 2.7 and python 3.4


Contributors
------------
Feel free to raise issues and send some pull request, we'll be happy to look at them!


License
-------
[Apache License Version 2.0](LICENSE)
