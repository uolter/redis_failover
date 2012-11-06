from distutils.core import setup
setup(name='redis_failover',
   version='1.0',
   requires = ["redis (>=2.7.1)", "zc.zk (>=2.4.4)", "zc_zookeeper_static (>=1.0.0)", "zktools (>=0.2.1)"],
   description='redis failover client with Apache Zookeeper integration',
   packages=['redis_failover'],
)

