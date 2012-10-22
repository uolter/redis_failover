# test_cluster.py

import unittest
from cluster import Cluster

class TestCluster(unittest.TestCase):

    def setUp(self):
        print "### setUp"
        self.cluster = Cluster()

    def tearDown(self):
        print "### tearDown"
    

if __name__ == '__main__':
    unittest.main()