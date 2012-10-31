'''
@author: mturatti
'''

import logging
import logging.config
logging.config.fileConfig("test_loggers_redis_monitor.conf")
logger = logging.getLogger("redis_monitor")

import unittest
from redis_monitor import RedisMonitor
import Queue
from utils import \
    ROLE_MASTER, ROLE_SLAVE, \
    REDIS_STATUS_OK, REDIS_STATUS_KO

HOST = "localhost"
PORT = 2181

class TestRedisMonitor(unittest.TestCase):

    def setUp(self):
        print "### setUp ###"
        self.monitor = RedisMonitor(zk=None, redis_hosts="localhost:6379,localhost:6389,localhost:6399", sleep_time=30, zk_path="/test/redis")
        self.monitor.zk = MockZooKeeper("zk")
        self.monitor.redis_class = MockRedis
        self.monitor.zk_properties = MockZooKeeper("zk_properties")
        self.monitor.queue = Queue.Queue()

    def test_all_ok(self):
        print "test_all_ok"
        master = self.monitor.cluster.add_node(HOST, PORT, ROLE_MASTER, REDIS_STATUS_OK)
        slave1 = self.monitor.cluster.add_node(HOST, PORT+1, ROLE_SLAVE, REDIS_STATUS_OK)
        slave2 = self.monitor.cluster.add_node(HOST, PORT+2, ROLE_SLAVE, REDIS_STATUS_OK)
        
        self.send_OK_Message(HOST, PORT)
        self.monitor._parse_message_from_queue()
        
        self.send_OK_Message(HOST, PORT+1)
        self.monitor._parse_message_from_queue()
        
        self.send_OK_Message(HOST, PORT+2)
        self.monitor._parse_message_from_queue()

        self.assertTrue(master.is_master())
        self.assertTrue(master.is_alive())
        
        self.assertTrue(slave1.is_slave())
        self.assertTrue(slave1.is_alive())
        
        self.assertTrue(slave2.is_slave())
        self.assertTrue(slave2.is_alive())

    def test_one_slave_KO(self):
        print "test_one_slave_KO"
        master = self.monitor.cluster.add_node(HOST, PORT, ROLE_MASTER, REDIS_STATUS_OK)
        slave1 = self.monitor.cluster.add_node(HOST, PORT+1, ROLE_SLAVE, REDIS_STATUS_OK)
        slave2 = self.monitor.cluster.add_node(HOST, PORT+2, ROLE_SLAVE, REDIS_STATUS_OK)
        
        self.send_OK_Message(HOST, PORT+1)
        self.monitor._parse_message_from_queue()
        
        self.send_OK_Message(HOST, PORT)
        self.monitor._parse_message_from_queue()
        
        self.send_KO_Message(HOST, PORT+2)
        self.monitor._parse_message_from_queue()

        self.assertTrue(master.is_master())
        self.assertTrue(master.is_alive())

        self.assertTrue(slave1.is_slave())
        self.assertTrue(slave1.is_alive())

        self.assertTrue(slave2.is_slave())
        self.assertFalse(slave2.is_alive())

    def test_master_KO(self):
        print "test_master_KO"
        master = self.monitor.cluster.add_node(HOST, PORT, ROLE_MASTER, REDIS_STATUS_OK)
        slave1 = self.monitor.cluster.add_node(HOST, PORT+1, ROLE_SLAVE, REDIS_STATUS_OK)
        slave2 = self.monitor.cluster.add_node(HOST, PORT+2, ROLE_SLAVE, REDIS_STATUS_OK)
        
        self.send_OK_Message(HOST, PORT+1)
        self.monitor._parse_message_from_queue()
        
        self.send_KO_Message(HOST, PORT)
        self.monitor._parse_message_from_queue()
        
        self.send_OK_Message(HOST, PORT+2)
        self.monitor._parse_message_from_queue()

        self.assertTrue(master.is_slave())
        self.assertFalse(master.is_alive())

        self.assertTrue(slave1.is_master())
        self.assertTrue(slave1.is_alive())

        self.assertTrue(slave2.is_slave())
        self.assertTrue(slave2.is_alive())

    def test_all_going_KO_master(self):
        print "test_all_going_KO_master"
        master = self.monitor.cluster.add_node(HOST, PORT, ROLE_MASTER, REDIS_STATUS_OK)
        slave1 = self.monitor.cluster.add_node(HOST, PORT+1, ROLE_SLAVE, REDIS_STATUS_OK)
        slave2 = self.monitor.cluster.add_node(HOST, PORT+2, ROLE_SLAVE, REDIS_STATUS_OK)
        
        # kill slave1
        self.send_KO_Message(HOST, PORT+1)
        self.monitor._parse_message_from_queue()

        self.assertTrue(slave1.is_slave())
        self.assertFalse(slave1.is_alive())
        
        # kill master
        self.send_KO_Message(HOST, PORT)
        self.monitor._parse_message_from_queue()

        self.assertTrue(master.is_slave())
        self.assertFalse(master.is_alive())
        
        # kill slave2
        self.send_KO_Message(HOST, PORT+2)
        self.monitor._parse_message_from_queue()

        self.assertTrue(slave2.is_master())
        self.assertFalse(slave2.is_alive())

        # slave2 is resurrected as master
        self.send_OK_Message(HOST, PORT+2)
        self.monitor._parse_message_from_queue()

        self.assertTrue(slave2.is_master())
        self.assertTrue(slave2.is_alive())

        # slave1 is resurrected as slave
        self.send_OK_Message(HOST, PORT+1)
        self.monitor._parse_message_from_queue()

        self.assertTrue(slave1.is_slave())
        self.assertTrue(slave1.is_alive())

    def test_all_going_KO_slave(self):
        print "test_all_going_KO_slave"
        master = self.monitor.cluster.add_node(HOST, PORT, ROLE_MASTER, REDIS_STATUS_OK)
        slave1 = self.monitor.cluster.add_node(HOST, PORT+1, ROLE_SLAVE, REDIS_STATUS_OK)
        slave2 = self.monitor.cluster.add_node(HOST, PORT+2, ROLE_SLAVE, REDIS_STATUS_OK)
        
        # kill slave1
        self.send_KO_Message(HOST, PORT+1)
        self.monitor._parse_message_from_queue()

        self.assertTrue(slave1.is_slave())
        self.assertFalse(slave1.is_alive())
        
        # kill master
        self.send_KO_Message(HOST, PORT)
        self.monitor._parse_message_from_queue()

        self.assertTrue(master.is_slave())
        self.assertFalse(master.is_alive())
        
        # kill slave2
        self.send_KO_Message(HOST, PORT+2)
        self.monitor._parse_message_from_queue()

        self.assertTrue(slave2.is_master())
        self.assertFalse(slave2.is_alive())

        # slave1 is resurrected
        self.send_OK_Message(HOST, PORT+1)
        self.monitor._parse_message_from_queue()

        self.assertTrue(slave1.is_master())
        self.assertTrue(slave1.is_alive())

        # slave2 is resurrected
        self.send_OK_Message(HOST, PORT+2)
        self.monitor._parse_message_from_queue()

        self.assertTrue(slave2.is_alive())
        self.assertTrue(slave2.is_slave())

    def test_all_resurrecting(self):
        print "test_all_resurrecting"
        master = self.monitor.cluster.add_node(HOST, PORT, ROLE_MASTER, REDIS_STATUS_KO)
        slave1 = self.monitor.cluster.add_node(HOST, PORT+1, ROLE_SLAVE, REDIS_STATUS_KO)
        slave2 = self.monitor.cluster.add_node(HOST, PORT+2, ROLE_SLAVE, REDIS_STATUS_KO)
        # kill slave1
        self.send_KO_Message(HOST, PORT+1)
        self.send_KO_Message(HOST, PORT)
        self.send_KO_Message(HOST, PORT+2)

        self.monitor._parse_message_from_queue()
        self.assertTrue(slave1.is_slave())
        self.assertFalse(slave1.is_alive())
        
        self.monitor._parse_message_from_queue()
        self.assertTrue(master.is_master())
        self.assertFalse(master.is_alive())
        
        self.monitor._parse_message_from_queue()
        self.assertTrue(slave2.is_slave())
        self.assertFalse(slave2.is_alive())

    def send_OK_Message(self, redis_host, redis_port):
        self.send_message(redis_host, redis_port, REDIS_STATUS_OK)

    def send_KO_Message(self, redis_host, redis_port):
        self.send_message(redis_host, redis_port, REDIS_STATUS_KO)

    def send_message(self, redis_host, redis_port, redis_status):
        message = self.create_message(redis_host, redis_port, redis_status)
        self.monitor.queue.put(message)

    def create_message(self, redis_host, redis_port, redis_status):
        worker_name = '%s:%s' % (redis_host, redis_port)
        text = "%s,%s" % (worker_name, redis_status)
        return text

    def tearDown(self):
        print "### tearDown ###"


class MockRedis(object):

    def __init__(self, *args, **kwargs):
        print "new MockRedis %s" % str(args)

    def slaveof(self, host=HOST, port=PORT):
        print "called slaveof: %s, %d" % (host, port)


class MockZooKeeper(object):

    def __init__(self, *args, **kwargs):
        print "new MockZooKeeper %s" % str(args)
    
    def update(self, master, slaves):
        print "called update: %s, %s" % (master, slaves)


if __name__ == '__main__':
    unittest.main()