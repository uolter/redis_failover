Redis Failover
==============

Python Redis failover is a solution based on Apache ZooKeeper.

__WARNING__: this code is still alpha quality. We are testing under Mac OSX and Linux, with Python 2.6 and 2.7.

Authors:
* Walter Traspadini <wtraspad@gmail.com> ([@uollter](https://twitter.com/uollter))
* Maurizio Turatti <maurizio.turatti@gmail.com> ([@mkj6](https://twitter.com/mkj6))

It's more or less the Python version of the redis_failover **Ruby solution** by [@ryanlecompte](https://twitter.com/ryanlecompte) you can get from here 
https://github.com/ryanlecompte/redis_failover and which inspired us.

At the time we put in place the solution the redis stable version 2.6.2 does not already have a failover mechanism 
to rely on. Since we needed a python version to run within a REST Api application build on top of Flask 
(and redis of course) we developed our own solution

The solution is made of three main components:

* [Apache ZooKeeper](http://zookeeper.apache.org/): is a centralized service for maintaining configuration information, 
naming, providing distributed synchronization, and providing group services.

* **RedisFailover** client: is the redis failover client which wraps the 'standard' redis client [redis-py](https://github.com/andymccurdy/redis-py) providing a failover mechanism and a load balancing between the master 
and slaves redis 'cluster'.

* **RedisMonitor**: is an high level sentinel responsible to watch over the redis nodes and promote a slave to master in case
of the starting master crashes.

Technical Architecture
======================

![Architecture diagram](https://github.com/uolter/redis_failover/raw/master/misc/RedisFailover.png)

Installation
============

Both the client and the monitor have **dependencies** on other python packages you can install with **pip** or 
**easy_install**. For the client we also provided a setup script to install the required modules inside the python path.


`pip install -r requirements.txt`

will install all the dependecies which are:

* redis >= 2.7.1                  # redis python api client
* zc-zookeeper-static >= 3.4.4    # ZooKeeper Python bindings
* zc.zk >= 1.0.0                  # high-level interfaces to the low-level zookeeper extension
* zktools >= 0.3                  # tools implementing higher level constructs such as Configuration and Lock

> Note: The fork https://github.com/mkjsix/zktools (v. 0.3dev) is a copy of zktools which has been tested to work with our software. Please __don't use__ zktool 0.2.1 as distributed via PyPi (http://pypi.python.org/pypi/zktools/0.2.1) because it's buggy (the zk lock eats all the CPU).

To quickly set up a ZooKeeper standalone server or a ZooKeeper cluster please refer to the official documentation 
**ZooKeeper Getting Started Guide**: http://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html

Since the RedisFailover client rely on a path the Monitor creates into ZooKeeper you must start Zookeeper first, then the
Redis Monitor and evenutaly you can use the client.

The Redis Monitor can be executed as a command line script providing a set of basic configurations as simple options:

    python start_monitor.py --help

    Usage: start_monitor.py [options]

    Options:
      -h, --help                              show this help message and exit

      -z, --zkhosts=ZK_HOSTS                  zookeeper list of host:port comma separated

      -p, --path=ZK_PATH                      zookeeper root path

      -r, --redishosts=RS_HOSTS               redis list of host:port comma separated

      -s, --sleeptime=SLEEP_TIME              waiting time in seconds between thread execution


    e.g:
    python start_monitor.py -p /redis/cluster -z localhost:2181,localhost:2182,localhost:2183 
    -r localhost:6379,localhost:6389,localhost:6399 -s 30

It will start a monitor which speaks to the ZooKeeper cluster identified with localhost:2181 localhost:2182 localhost:2183 and will check the redis instances identified with localhost:6379 localhost:6389 localhost:6399. 
Every 30 sec each worker (process) sends a keep alive message to the master process which records the status and role (master / slave) of the redis servers to the ZooKeeper path /redis/cluster


Now, that the monitor is running and above all it notified to ZooKeeper the redis cluster scheme and status you can start playing with the client.

just open a python console:


    from redis_failover import RedisFailover

    rs = RedisFailover(hosts='localhost:2181,localhost:2182,localhost:2183', zk_path='/redis/cluster')

    # celebrate !!!! rs is now the redis client … try some methods
    rs.keys()
    >>>

    ['ACTIVITY:max:QOL:20121017',
     'ACTIVITY:bob:QOL:20121029',
     'SERVICES',
     'AUTH:admin:secret',
     'SERVICE:QOL',
     ]

The client accepts as input parametes the list of ZooKeeper servers (comma separated) and the ZooKeeper path where
he can get the list of redis servers.
Behind the hood it also creates a connection pool for each readis server. 
Hereafter it is possibile to use all the redis commands as documented here: https://github.com/andymccurdy/redis-py
since as already pointed out the failover client is more or less a wrapper of the standard python redis client.

Status
======

You can check the build history and unit test status on drone.io [![](https://drone.io/uolter/redis_failover/status.png)](https://drone.io/uolter/redis_failover/latest)

License
=======

Licensed under the Apache License, Version 2.0. See LICENSE for details.

Copyright 2012 Sourcesense http://www.sourcesense.com

