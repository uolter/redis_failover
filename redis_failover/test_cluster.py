# test_cluster.py

import unittest
from cluster import Cluster
from utils import *

HOST = "localhost"
PORT = 1000

class TestCluster(unittest.TestCase):

    def setUp(self):
        print "### setUp ###"
        self.cluster = Cluster()

    def test_add_single_master(self):
        print "test_add_single_master"
        self.cluster.add_node(host=HOST, port=PORT, role=ROLE_MASTER, status=REDIS_STATUS_OK)
        self.assertEqual(self.cluster.size(), 1)
        node = self.cluster.get_master()
        self.assertTrue(node.is_master())
        self._compare_node(node, role=ROLE_MASTER, status=REDIS_STATUS_OK)


    def test_add_single_slave(self):
        print "test_add_single_slave"
        self.cluster.add_node(host=HOST, port=PORT, role=ROLE_SLAVE, status=REDIS_STATUS_OK)
        self.assertEqual(self.cluster.size(), 1)
        self.assertIsNone(self.cluster.get_master())
        node = self.cluster.get_node(host=HOST, port=PORT)
        self._compare_node(node, role=ROLE_SLAVE, status=REDIS_STATUS_OK)


    def test_promote_new_master_single_node(self):
        print "test_promote_new_master_single_node"
        self.cluster.add_node(host=HOST, port=PORT, role=ROLE_MASTER, status=REDIS_STATUS_OK)
        self.assertEqual(self.cluster.size(), 1)
        node = self.cluster.get_master()
        self.assertTrue(node.is_master())
        self.cluster.promote_new_master(node)
        self._compare_node(node, role=ROLE_MASTER, status=REDIS_STATUS_KO)

    def test_promote_new_master_all_dead(self):
        print "test_promote_new_master_all_dead"
        self.cluster.add_node(host=HOST, port=PORT, role=ROLE_MASTER, status=REDIS_STATUS_KO)
        self.cluster.add_node(host=HOST, port=PORT+1, role=ROLE_SLAVE, status=REDIS_STATUS_KO)
        self.cluster.add_node(host=HOST, port=PORT+2, role=ROLE_SLAVE, status=REDIS_STATUS_KO)
        self.assertEqual(self.cluster.size(), 3)
        old_master = self.cluster.get_master()
        self.assertTrue(old_master.is_master())
        self.cluster.promote_new_master(old_master)
        new_master = self.cluster.get_master()
        self.assertTrue(old_master == new_master)
        self.assertFalse(new_master.is_alive())


    def _compare_node(self, node, host=HOST, port=PORT, role=None, status=None):
        self.assertEqual(node.role, role)
        self.assertEqual(node.status, status)
        self.assertEqual(node.host, host)
        self.assertEqual(node.port, port)


    def tearDown(self):
        print "### tearDown ###"
        self.cluster = None
    

if __name__ == '__main__':
    unittest.main()