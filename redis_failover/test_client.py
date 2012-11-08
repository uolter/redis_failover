'''
Created on Nov 6, 2012

@author: uolter
'''
import unittest
import client
from redis.exceptions import ConnectionError


from client import RedisFailover


class ConnectionPoolMock:
    
    def __init__(self,host='localhost', port=6379, db=0):
        
        self.host = host
        self.port = port
        self.db = db

class RedisMock():
    
    def __init__(self, host='localhost', port=6379, connection_pool = None, db=0):
        
        self.host = host
        self.port = port
        self.db = db
        self.repository = {0: {'ok':'ok'}, 1:{}}
        
        if connection_pool:
            self.host = connection_pool.host
            self.port = connection_pool.port
            self.db = connection_pool.db
    
    def set(self, key, value):
       
        self.repository[self.db][key] = value
        return 'OK'
    
    def get(self, key):
        
        return self.repository.get(self.db).get(key)

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
        client.redis.ConnectionPool = ConnectionPoolMock
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
            
         
    def test_db_1(self):
        '''
            it writes and reads on redis db 1 instead of the default db 0
        '''
        
        # it uses here only the master since there is no propagating of data among redis instances
        
        self.rs1 = RedisFailover('localhost:8888', '/redis/cluster', db=1)
        
        self.rs1.zk.repository = {'/redis/cluster':{"master":["localhost:6399","OK"],
                           "slaves":{"0":["localhost:6389","KO"],
                                     "1":["localhost:6379","KO"]}}}
        
        self.rs1._setup_redis_master()
        self.rs1._setup_redis_slaves()
        
        resp = self.rs1.set('db1', 'val_db_1')
         
        self.assertEquals(resp, 'OK', 'Response ok')
        
        resp = self.rs1.get('db1')
            
        self.assertEquals(resp, 'val_db_1', 'Response ok')
        
        # now I read on db 0
        
        resp = self.rs.get('db1')   
        self.assertFalse(resp=='val_db_1')
        
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()