[![Build Status](https://travis-ci.org/internap/netman.svg?branch=master)](https://travis-ci.org/internap/redlock-fifo)
[![PyPI version](https://badge.fury.io/py/redlock-fifo.svg)](http://badge.fury.io/py/redlock-fifo)

Redlock-Fifo
============

Redlock-Fifo makes [Redlock-py](https://github.com/SPSCommerce/redlock-py) usable in a multi-threaded environment.

The project make sure that the requester for a lock will have it according to the order it asked for access. It also 
adds the ability to extends a lock without having to release the lock.

Why would I use this ?
----------------------
The normal behavior of redlock-py is to `return false` on a existing lock. The mainstream approach is to 
poll the lock until acquisition. This process has two downsides :

1. It doesn't warranty that the first requester will be the first to get the lock.
2. It doesn't warranty an access to the resource (a requester can wait indefenitely).

To prevent [starvation](https://en.wikipedia.org/wiki/Starvation_%28computer_science%29), we add a simple FIFO queue 
implemented in Redis managing access to the resource. This queue allows us to warranty access to a resource and warranty
 that the lock will be attribued in order of lock request.

You can use this project in multi-threaded environment to prevent process starvation and warranty order of execution.

Requirements
------------

 * python 2.7
 * tox
 
Installation
------------

    python setup.py install

Usage
-----

    # create a lock manager
    lockmanager = FIFORedlock([{“host”: “localhost”, “port”: 6379, “db”: 0}]) 
    
    # Acquire a lock for 1000ms
    my_lock = lockmanager.lock(“my_resource_name”, 1000)
    
    # To extend a lock for another 1000ms
    lockmanager.extend(my_lock, 1000)
    
    # To release a lock
    lockmanager.unlock(my_lock)

These default parameters are used at initialization of `FIFORedlock` to customize the FIFO queue: 

    retry_count=1
    retry_delay=0.2
    fifo_retry_count=30
    fifo_retry_delay=0.2
    fifo_queue_length=64

Tests
-----

    tox -r
    
These tests make sure that the lock algorithm is reliable.

Contributors
------------
Feel free raise issues and send some pull request, we'll be happy to look at them!

License
-------
[Apache License Version 2.0](LICENSE)
