Redis Failover
==============

Python Redis failover is a solution based on Apache ZooKeeper.

__WARNING__: this code is still alpha quality. We are testing under Mac OSX and Linux, with Python 2.6 and 2.7.

Authors:
* Walter Traspadini <wtraspad@gmail.com>
* Maurizio Turatti <maurizio.turatti@gmail.com>

It's more or less the python version of the Ruby solution you can get from here 
https://github.com/ryanlecompte/redis_failover and which inspired us.

At the time we put in place the solution the redis stable version 2.6.2 does not already have a failover mechanism 
to rely on. Since we needed a python version to run within a REST Api application build on top of Flask 
(and redis of course) we developed our own solution


Mainly, we have three components:

* Apache ZooKeeper (http://zookeeper.apache.org/): is a centralized service for maintaining configuration information, 
naming, providing distributed synchronization, and providing group services.

* RedisFailover client: is the redis failover client which wraps the 'standard' redis client redis-py 
(https://github.com/andymccurdy/redis-py) providing a failover mechanism and a load balancing between the master 
and slaves redis 'cluster'.

* RedisMonitor: is an high level sentinel responible to watch over the redis nodes and promote a slave to master in case
of the starting master crashes.


License:
=======

Licensed under the Apache License, Version 2.0. See LICENSE for details.

Copyright 2012 Sourcesense http://www.sourcesense.com



