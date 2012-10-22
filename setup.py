from distutils.core import setup
setup(name='redis_failover',
   version='1.0',
   requires = ["redis", "zc.zk",  "zktools"],
   description='redis failover client with Apache Zookeeper integration',
   packages=['redis_failover'],
)

