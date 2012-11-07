'''
Created on Oct 8, 2012
@author: uolter
'''

import redis
import random
import zc.zk
from utils import REDIS_PATH, REDIS_STATUS_OK

SLAVES = "slaves"
MASTER = "master"


class RedisFailover():
    '''
        This client extends the standard redis-py client in order to provide a fail-over mechanism 
        based on Apache ZooKeeper (http://zookeeper.apache.org/)
        It also balances all the write calls to hit the master node and randomly the read calls to 
        the available set of slaves. 
    '''
    
    # read only method allowed
    _read_keys = {
        'debug': 'debug', 'getbit': 'getbit', 'keys': 'keys',
        'get': 'get', 'getrange': 'getrange', 'hget': 'hget',
        'hgetall': 'hgetall', 'hkeys': 'hkeys', 'hlen': 'hlen', 'hmget': 'hmget',
        'hvals': 'hvals', 'info': 'info', 'lindex': 'lindex', 'llen': 'llen',
        'lrange': 'lrange', 'object': 'object',
        'scard': 'scard', 'sismember': 'sismember', 'smembers': 'smembers',
        'srandmember': 'srandmember', 'strlen': 'strlen', 'type': 'type',
        'zcard': 'zcard', 'zcount': 'zcount', 'zrange': 'zrange', 'zrangebyscore': 'zrangebyscore',
        'zrank': 'zrank', 'zrevrange': 'zrevrange', 'zrevrangebyscore': 'zrevrangebyscore',
        'zrevrank': 'zrevrank', 'zscore': 'zscore',
        'mget': 'mget', 'bitcount': 'bitcount', 'echo': 'echo', 'debug_object': 'debug_object',
        'save': 'save', 'substr': 'substr'
    }

    # write 'only' method allowed
    _write_keys = {
        'append': 'append', 'blpop': 'blpop', 'brpop': 'brpop', 'brpoplpush': 'brpoplpush',
        'decr': 'decr', 'decrby': 'decrby', 'del': 'del', 'exists': 'exists', 'hexists': 'hexists',
        'expire': 'expire', 'expireat': 'expireat', 'pexpire': 'pexpire', 'pexpireat': 'pexpireat', 'getset': 'getset', 'hdel': 'hdel',
        'hincrby': 'hincrby', 'hincrbyfloat': 'hincrbyfloat', 'hset': 'hset', 'hsetnx': 'hsetnx', 'hmset': 'hmset',
        'incr': 'incr', 'incrby': 'incrby', 'incrbyfloat': 'incrbyfloat', 'linsert': 'linsert', 'lpop': 'lpop',
        'lpush': 'lpush', 'lpushx': 'lpushx', 'lrem': 'lrem', 'lset': 'lset',
        'ltrim': 'ltrim', 'move': 'move', 'pipeline': 'pipeline',  
        'persist': 'persist', 'publish': 'publish', 'psubscribe': 'psubscribe', 'punsubscribe': 'punsubscribe',
        'rpop': 'rpop', 'rpoplpush': 'rpoplpush', 'rpush': 'rpush',
        'rpushx': 'rpushx', 'save': 'save', 'sadd': 'sadd', 'sdiff': 'sdiff', 'sdiffstore': 'sdiffstore',
        'set': 'set', 'setbit': 'setbit', 'setex': 'setex', 'setnx': 'setnx',
        'setrange': 'setrange', 'sinter': 'sinter', 'sinterstore': 'sinterstore', 'smove': 'smove',
        'sort': 'sort', 'spop': 'spop', 'srem': 'srem', 'subscribe': 'subscribe',
        'sunion': 'sunion', 'sunionstore': 'sunionstore', 'unsubscribe': 'unsubscribe', 'unwatch': 'unwatch',
        'watch': 'watch', 'zadd': 'zadd', 'zincrby': 'zincrby', 'zinterstore': 'zinterstore',
        'zrem': 'zrem', 'zremrangebyrank': 'zremrangebyrank', 'zremrangebyscore': 'zremrangebyscore', 'zunionstore': 'zunionstore',
        'mset': 'mset', 'msetnx': 'msetnx', 'rename': 'rename', 'renamenx': 'renamenx',
        'del': 'del', 'delete': 'delete', 'ttl': 'ttl', 'flushall': 'flushall', 'flushdb': 'flushdb',
    }
    
    
    def __init__(self, hosts='localhost:2181', zk_path=REDIS_PATH):
         
        self.hosts=hosts # zookeeper hosts list.
        self.zk= zc.zk.ZooKeeper(self.hosts)
        self.zk_path =zk_path
             
        @self.zk.properties(self.zk_path)
        def my_data(p):
            '''
                listener to zk node value changes
            ''' 
            self._setup_redis_master()
            self._setup_redis_slaves()
              
        self.callback = my_data
        
   
    def _setup_redis_slaves(self):
                
        slaves = self.zk.get_properties(self.zk_path)[SLAVES]
        
        self.host_slaves = []
        for s in slaves:
            host = slaves[s][0]
            status = slaves[s][1]
            if (host and status and status == REDIS_STATUS_OK):
                self.host_slaves.append(host) # slave avalible for reading.
        
        # redis slaves connection pool list
        self.redis_slaves = []
        
        for slave in self.host_slaves:
            pool = redis.ConnectionPool(host=slave.split(':')[0], 
                                        port=int(slave.split(':')[1]))
            
            self.redis_slaves.append(redis.Redis(connection_pool=pool))
        
               
    def _setup_redis_master(self):
        
        self.host_master = self.zk.get_properties(self.zk_path)[MASTER][0]
         
        # connection pool to the master node.
        pool = redis.ConnectionPool(host=self.host_master.split(':')[0], 
                                    port=int(self.host_master.split(':')[1]))
        
        self.redis_master = redis.Redis(connection_pool=pool)
         
             
    def __getattr__(self, name, *args, **kwargs):
        
        '''
            Magic method that proxies every call to Reids api as defined in redis-py
        ''' 
        
        def function( *args, **kwargs):
            
            if name in self._write_keys:
                # hits the master node.
                return getattr(self.redis_master, name)(*args, **kwargs)
            
            elif name in self._read_keys:
                if len(self.redis_slaves) == 0:
                    # no slave available then it reads from the master.
                    return getattr(self.redis_master, name)(*args, **kwargs)
                
                # read from one of the slaves.
                return getattr(self.redis_slaves[random.randint(0, len(self.redis_slaves)-1)], name)(*args, **kwargs)
            else:
                raise TypeError('Unknown method [%s]' %name)
           
        return function
