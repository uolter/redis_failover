'''
@author: mturatti
''' 

import unittest
from cluster import Cluster
from utils import \
    ROLE_MASTER, ROLE_SLAVE, \
    REDIS_STATUS_OK, REDIS_STATUS_KO

HOST = "localhost"
PORT = 1000

class TestCluster(unittest.TestCase):

    def setUp(self):
        print "### setUp ###"
        self.cluster = Cluster()

    def test_make_key(self):
        print "test_make_key"
        text = "%s:%s" % (HOST, PORT)
        self.assertEqual(self.cluster._make_key(HOST, PORT), text)

    def test_set_role(self):
        self.cluster.add_node(host=HOST, port=PORT, role=ROLE_MASTER, status=REDIS_STATUS_OK)
        self.cluster._set_role(HOST, PORT, ROLE_MASTER)
        node = self.cluster.get_node(HOST, PORT)
        self.assertTrue(node.is_master)

    def test_filtered_list(self):
        print "test_filtered_list"
        self.cluster.add_node(host=HOST, port=PORT, role=ROLE_MASTER, status=REDIS_STATUS_OK)
        self.cluster.add_node(host=HOST, port=PORT+1, role=ROLE_SLAVE, status=REDIS_STATUS_KO)
        self.cluster.add_node(host=HOST, port=PORT+2, role=ROLE_SLAVE, status=REDIS_STATUS_OK)
        testlist = self.cluster.filtered_list(roles=(ROLE_MASTER))
        self.assertEqual(len(testlist), 1)
        self.assertTrue(testlist[0].is_master())
        self.assertTrue(testlist[0].is_alive())
        testlist = self.cluster.filtered_list(roles=(ROLE_SLAVE))
        self.assertEqual(len(testlist), 2)
        self.assertTrue(testlist[0].is_slave() and testlist[1].is_slave())
        testlist = self.cluster.filtered_list()
        self.assertEqual(len(testlist), 3)
        testlist = self.cluster.filtered_list(status=REDIS_STATUS_KO)
        self.assertEqual(len(testlist), 1)
        self.assertTrue(testlist[0].is_slave() and not testlist[0].is_alive())
        testlist = self.cluster.filtered_list(status=REDIS_STATUS_OK)
        self.assertEqual(len(testlist), 2)
        testlist = self.cluster.filtered_list(roles=ROLE_MASTER, status=REDIS_STATUS_KO)
        self.assertEqual(len(testlist), 0)
        testlist = self.cluster.filtered_list(roles=(ROLE_MASTER,ROLE_SLAVE), status=REDIS_STATUS_KO)
        self.assertEqual(len(testlist), 1)
        self.assertTrue(testlist[0].is_slave() and not testlist[0].is_alive())

    def test_add_single_master(self):
        print "test_add_single_master"
        self.cluster.add_node(host=HOST, port=PORT, role=ROLE_MASTER, status=REDIS_STATUS_OK)
        self.assertEqual(len(self.cluster), 1)
        node = self.cluster.get_master()
        self.assertTrue(node.is_master() and node.is_alive())

    def test_add_single_slave(self):
        print "test_add_single_slave"
        self.cluster.add_node(host=HOST, port=PORT, role=ROLE_SLAVE, status=REDIS_STATUS_OK)
        self.assertEqual(len(self.cluster), 1)
        self.assertIsNone(self.cluster.get_master())
        node = self.cluster.get_node(host=HOST, port=PORT)
        self.assertTrue(node.is_slave() and node.is_alive())

    def test_promote_new_master_single_node(self):
        print "test_promote_new_master_single_node"
        old_master = self.cluster.add_node(host=HOST, port=PORT, role=ROLE_MASTER, status=REDIS_STATUS_OK)
        self.assertEqual(len(self.cluster), 1)
        self.cluster.promote_new_master(old_master)
        node = self.cluster.get_master()
        self.assertTrue(node is not None)
        self.assertTrue(node.is_master())
        self.assertFalse(node.is_alive())

    def test_str(self):
        print "test_str"
        self.cluster.add_node(host=HOST, port=PORT, role=ROLE_MASTER, status=REDIS_STATUS_OK)
        self.cluster.add_node(host=HOST, port=PORT+1, role=ROLE_SLAVE, status=REDIS_STATUS_OK)
        self.assertEqual("(localhost:1000,master,OK),(localhost:1001,slave,OK)", str(self.cluster))

    def _create_message(self, redis_host, redis_port, redis_status):
        worker_name = '%s:%s' % (redis_host, redis_port)
        text = "%s,%s" % (worker_name, redis_status)
        return text

    def tearDown(self):
        print "### tearDown ###"
        self.cluster = None
    

if __name__ == '__main__':
    unittest.main()
    