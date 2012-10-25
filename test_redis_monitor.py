# test_redis_monitor

import unittest
from redis_monitor import RedisMonitor
import Queue
from redis_failover.utils import *

class Mock():

    def __init__(self, *args, **kwargs):
        self.class_name = args[0]

    def __getattr__(self, name, *args, **kwargs):
        def function(*args, **kwargs):
            return (name, args)
        return function


class TestRedisMonitor(unittest.TestCase):

    def setUp(self):
        print "### setUp ###"
        def myfunc():
            pass
        zk_mock = Mock("ZooKeeper")
        redis_mock = Mock("redis.Redis")
        self.monitor = RedisMonitor(zk=zk_mock, redis_hosts="localhost:6379,localhost:6389,localhost:6399", zk_path="/test/redis")
        self.monitor.redis_class = redis_mock("localhost:2181,localhost:2182")
        self.monitor.queue = Queue.Queue()
        #self.monitor.discover_redis = myfunc


    def test_first(self):
        print "test_first"
        message = self._create_message("localhost", 8080, REDIS_STATUS_OK)
        self.monitor.queue.put(message)
        self.monitor.cluster.add_node("localhost", 8080, ROLE_MASTER, REDIS_STATUS_OK)
        self.monitor._parse_messages()
        
    def _create_message(self, redis_host, redis_port, redis_status):
        worker_name = '%s:%s' % (redis_host, redis_port)
        text = "%s,%s" % (worker_name, redis_status)
        return text

    def tearDown(self):
        print "### tearDown ###"


if __name__ == '__main__':
    unittest.main()