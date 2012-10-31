Redis Failover
==============

Python Redis failover is a solution based on Apache ZooKeeper.

__WARNING__: this code is still alpha quality. We are testing under Mac OSX and Linux, with Python 2.6 and 2.7.

Authors:
* Walter Traspadini <wtraspad@gmail.com> (https://twitter.com/uollter)
* Maurizio Turatti <maurizio.turatti@gmail.com> (https://twitter.com/mkj6)

It's more or less the python version of the Ruby solution you can get from here 
https://github.com/ryanlecompte/redis_failover and which inspired us.

At the time we put in place the solution the redis stable version 2.6.2 does not already have a failover mechanism 
to rely on. Since we needed a python version to run within a REST Api application build on top of Flask 
(and redis of course) we developed our own solution


Mainly, we have three components:

* **Apache ZooKeeper** (http://zookeeper.apache.org/): is a centralized service for maintaining configuration information, 
naming, providing distributed synchronization, and providing group services.

* **RedisFailover** client: is the redis failover client which wraps the 'standard' redis client redis-py 
(https://github.com/andymccurdy/redis-py) providing a failover mechanism and a load balancing between the master 
and slaves redis 'cluster'.

* **RedisMonitor**: is an high level sentinel responsible to watch over the redis nodes and promote a slave to master in case
of the starting master crashes.

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

License
=======

Licensed under the Apache License, Version 2.0. See LICENSE for details.

Copyright 2012 Sourcesense http://www.sourcesense.com



