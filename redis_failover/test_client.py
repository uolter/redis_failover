'''
Created on Nov 6, 2012

@author: uolter
'''
import unittest
import client
from redis.exceptions import ConnectionError


from client import RedisFailover


class RedisMock():
    
    def __init__(self, host='localhost', port=6379, connection_pool = None):
        
        self.host = host
        self.port = port
        self.repository = {'ok':'ok'}
        self.connection_pool = connection_pool
    
    def set(self, key, value):
        
        self.repository[key] = value
        
        return 'OK'
    
    def get(self, key):
        
        return self.repository.get(key)

class RedisConnectionErrorMock(RedisMock):    
    def get(self, key):
        
        raise ConnectionError()
 
class Properties:
    
    def __init__(self, *args):
        pass

    def __call__(self, func):
        
        return self

class ZooKeeperMock():
    
    def __init__(self, hosts):
        self.hosts = hosts
        
        self.repository = {'/redis/cluster':{"master":["localhost:6379","OK"],
                           "slaves":{"0":["localhost:6389","OK"],
                                     "1":["localhost:6399","OK"]}}}
        
    def properties(self, path):
        return Properties(self, path)
    
    def get_properties(self, key):
        
        return self.repository[key]
        

class TestClient(unittest.TestCase):

    def setUp(self):
        
        client.zc.zk.ZooKeeper = ZooKeeperMock
        client.redis.Redis = RedisMock
        self.rs = RedisFailover('localhost:8888', '/redis/cluster')
        
        self.rs._setup_redis_master()
        self.rs._setup_redis_slaves()

    def test_setup_master_slave(self):
        
        self.assertTrue( isinstance(self.rs.redis_master, RedisMock))
        self.assertEqual( len(self.rs.redis_slaves), 2)

    def test_set(self):
         
        resp = self.rs.set('test', 'ok')
        
        self.assertEquals(resp, 'OK', 'Response ok')
        
    def test_get_ko(self):
        
        resp = self.rs.get('test')        
        self.assertNotEqual(resp, 'OK', 'Response ok')
        
    def test_get_ok(self):
        
        resp = self.rs.get('ok')        
        self.assertEqual(resp, 'ok', 'Response ok')
        
    def test_slave_ko(self):
        
        self.assertEqual( len(self.rs.redis_slaves), 2)
        
        self.rs.zk.repository = {'/redis/cluster':{"master":["localhost:6379","OK"],
                           "slaves":{"0":["localhost:6389","KO"],
                                     "1":["localhost:6399","OK"]}}}
        
        self.rs._setup_redis_slaves()
        
        # the first slave is now KO
        self.assertEqual( len(self.rs.redis_slaves), 1)
        
        # reads methods should work fine on 
        resp = self.rs.get('ok')        
        self.assertEqual(resp, 'ok', 'Response ok')
        
    def test_slaves_ko(self):
        
        # both slaves going down. Reading is still possible via the master
        
        self.rs.zk.repository = {'/redis/cluster':{"master":["localhost:6379","OK"],
                           "slaves":{"0":["localhost:6389","KO"],
                                     "1":["localhost:6399","KO"]}}}
        
        self.rs._setup_redis_slaves()
        
        # the first slave is now KO
        self.assertEqual( len(self.rs.redis_slaves), 0)
        
        # reads methods should work fine on the master anyway.!!
        resp = self.rs.get('ok')        
        self.assertEqual(resp, 'ok', 'Response ok')


    def test_swith_master_ok(self):
        
        # this should never happen
        
        self.rs.zk.repository = {'/redis/cluster':{"master":["localhost:6399","OK"],
                           "slaves":{"0":["localhost:6389","OK"],
                                     "1":["localhost:6379","OK"]}}}
        
        self.rs._setup_redis_master()
        self.rs._setup_redis_slaves()
                
        # reads methods should work fine on the slaves.!!
        resp = self.rs.get('ok')        
        self.assertEqual(resp, 'ok', 'Response ok')
        
        # set should fail
        resp = self.rs.set('test', 'ok')       
        self.assertEquals(resp, 'OK', 'Response ok')
        
    def test_swith_master_ko(self):
        
        client.redis.Redis = RedisConnectionErrorMock
        self.rs = RedisFailover('localhost:8888', '/redis/cluster')
        
        self.rs.zk.repository = {'/redis/cluster':{"master":["localhost:6399","KO"],
                           "slaves":{"0":["localhost:6389","KO"],
                                     "1":["localhost:6379","KO"]}}}
        
        self.rs._setup_redis_master()
        self.rs._setup_redis_slaves()
                
        # everything is down :-( 
        try:
            self.assertRaises(ConnectionError, self.rs.get('ok') )
        
      
            # set should fail
            self.assertRaises(ConnectionError, self.rs.set('test', 'ok'))
       
        except ConnectionError, e:
            print e


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()