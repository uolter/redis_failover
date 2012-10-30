'''
@author: mturatti
'''

from multiprocessing import Process, Queue
from time import sleep
import redis
from redis.exceptions import ConnectionError
from zc.zk import OPEN_ACL_UNSAFE
from utils import REDIS_STATUS_OK, REDIS_STATUS_KO, ROLE_SLAVE
from cluster import Cluster
import logging

REDIS_MONITOR_DUMMY_LIST = 'redis_monitor_dummy_list'

logger = logging.getLogger("redis_monitor")


class RedisMonitor(object):
    ''' The main monitor object. It starts all the Workers and 
        changes the cluster configuration in ZooKeeper when a Worker
        send an update message.'''

    def __init__(self, zk, redis_hosts, sleep_time, zk_path):
        self.redis_class = redis.Redis
        self.zk = zk
        self.cluster = Cluster()
        self.queue = Queue()
        self.redis_hosts = redis_hosts
        self.zk_properties = None
        self.list_of_workers = []
        self.zk_path = zk_path
        self.redis_servers_map = {}
        self.sleep_time = sleep_time

    def execute(self):
        ''' execute is the only public method. It starts the main loop.'''
        logger.info("Enter")
        logger.info("self.redis_class=%s", str(self.redis_class))
        self._discover_redis()
        while 1:
            self._parse_message_from_queue()
            self._check_all_workers()
        logger.info("Exit")

    def _discover_redis(self):
        ''' check all the redis instances and registers to zk host, port and status.
            It also prepares all the worker processes for monitoring.'''
        logger.info('Enter')
        self.zk.create_recursive(self.zk_path, '', OPEN_ACL_UNSAFE)
        self.zk_properties = self.zk.properties(self.zk_path)
        self._build_list_of_workers()
        (master_as_tuple, list_of_slaves) = self._get_cluster_representation()
        logger.info('master: [%r]', master_as_tuple[0])
        slaves_map = {}
        for i, slave in enumerate(list_of_slaves):
            slaves_map[i] = ('%s:%s' % (slave.host, slave.port), slave.status)
        logger.info("slaves: [%r] ", slaves_map)
        self._update_zk_properties(master = master_as_tuple, slaves = slaves_map)
        # start all workers
        for worker in self.list_of_workers:
            worker.start()
        logger.info("%d Workers created", len(self.list_of_workers))
        logger.info("Exit")

    def _get_cluster_representation(self):
        master = self.cluster.get_master()
        master_as_tuple = ('%s:%s' %(master.host, master.port), master.status)
        list_of_slaves = self.cluster.filtered_list((ROLE_SLAVE, ))
        return (master_as_tuple, list_of_slaves)

    def _parse_message_from_queue(self):
        ''' Blocks on queue, receives and reads messages from workers,
            execute update actions based on past and present cluster's status.'''
        message = self.queue.get()
        logger.debug("Received message from Worker: [%s]", message)
        server, new_status = message.split(',')
        host, port = server.split(':')
        present_node = self.cluster.get_node(host, port)
        old_status = present_node.status
        if new_status == REDIS_STATUS_KO:
            present_node.setKO()
            if old_status == REDIS_STATUS_OK:
                logger.warn("Node (%s) [%s] has DIED!", present_node.role, server)
                if present_node.is_master():
                    logger.warn("Master is down: promoting a new master...")
                    self._promote_new_master(present_node)
                self._update_zk()
        elif new_status == REDIS_STATUS_OK:
            present_node.setOK()
            if old_status == REDIS_STATUS_KO:
                logger.warn("Node (%s) [%s] has RESURRECTED!", present_node.role, server)
                redis_server = self._get_redis_server(host=present_node.host, port=present_node.port)
                master = self.cluster.get_master()
                if master.is_alive():
                    redis_server.slaveof(master.host, master.port)
                else:
                    self._promote_new_master(master)
                self._update_zk()
        else:
            logger.critical("Worker sent an unknown status: [%r]", new_status)

    def _check_all_workers(self):
        ''' check if all worker daemons are still alive,
            if not then re-start.'''
        for worker in self.list_of_workers:
            if not worker.get_process().is_alive():
                logger.error("Process [%s] has died, restarting it...", worker.get_process().name)
                worker.start()

    def _get_redis_server(self, host, port):
        ''' Loookup the redis_class from a map. If not there,
            creates and put in the map.'''
        key = "%s:%d" % (host, port)
        conn = None
        if key in self.redis_servers_map:
            conn = self.redis_servers_map[key]
        else:
            conn = self.redis_class(host, port)
            self.redis_servers_map[key] = conn
        return conn

    def _build_list_of_workers(self):
        ''' Add nodes to cluster, for each host:port received from configuration.'''
        for i, r in enumerate(self.redis_hosts):
            role=ROLE_SLAVE
            host = r.split(':')[0]
            port = int(r.split(':')[1])
            try:
                role = self._get_redis_server(host=host, port=port).info()['role']
                self.cluster.add_node(host, port, role, REDIS_STATUS_OK)
            except ConnectionError:
                logger.error("Node [%r] not available !!", r)
                self.cluster.add_node(host, port, role, REDIS_STATUS_KO)
            finally:
                redis_server = self._get_redis_server(host=host, port=port)
                self.list_of_workers.append(Worker(redis_server, host, port, self.queue, self.sleep_time))

    def _promote_new_master(self, old_master):
        ''' Promotes the first available node as new master,
            if the old master died and if at least one node is still alive.
            All the remaining nodes are set as slaves of the new master.'''
        logger.info("Enter")
        new_master = self.cluster.promote_new_master(old_master)
        if new_master.is_alive():
            redis_master_server = self._get_redis_server(host=new_master.host, port=new_master.port)
            # set this node as the master
            redis_master_server.slaveof()
            # set all the other alive nods as slaves of the new master
            list_of_OK_slaves = self.cluster.filtered_list(roles=(ROLE_SLAVE,), status=(REDIS_STATUS_OK,))
            for slave in list_of_OK_slaves:
                redis_slave_server = self._get_redis_server(host=slave.host, port=slave.port)
                redis_slave_server.slaveof(host=new_master.host, port=new_master.port)
        else:
            logger.critical("*** Can't promote a new master: all nodes are down! ***")
        logger.info("Exit")

    def _update_zk(self):
        ''' Updates ZooKeeper with the new cluster's configuration.'''
        logger.info("Enter")
        (master_as_tuple, list_of_slaves) = self._get_cluster_representation()
        logger.info("Master: [%r]", master_as_tuple)
        slaves_map = {}
        for i, slave in enumerate(list_of_slaves):
            slaves_map[i] = ("%s:%s" % (slave.host, slave.port), slave.status)
        logger.info("Slaves: [%r]", slaves_map)
        logger.warn("Updating ZooKeeper...")
        self._update_zk_properties(master = master_as_tuple, slaves = slaves_map)
        logger.info("Exit")

    def _update_zk_properties(self, master, slaves):
        ''' Calls the update action on the configured zk_path'''
        self.zk_properties.update(master = master, slaves = slaves)

    def __len__(self):
        return len(self.cluster)

    def __str__(self):
        return str(self.cluster)


class Worker(object):
    ''' A Worker instance is a child process which monitors a single redis server.
        It uses the redis BLPOP blocking call to test for connection. The call returns
        immediately with an exception if the connection to the redis server is lost.'''

    def __init__(self, redis_server, redis_host, redis_port, queue, sleep_time):
        self.redis_server = redis_server
        self.redis_host = redis_host
        self.redis_port = int(redis_port)
        self.queue = queue
        self.worker_name = '%s:%s' % (self.redis_host, self.redis_port)
        self.process = None
        self.sleep_time = sleep_time

    def start(self):
        ''' Start the single child process as daemon.'''
        self.process = Process(target=self.worker, name=self.worker_name, 
            args=(self.redis_server, self.worker_name, self.redis_host, self.redis_port, self.sleep_time))
        self.process.daemon = True
        self.process.start()

    def get_process(self):
        return self.process

    def send_message(self, redis_status):
        ''' Publish a message on the shared, synchronized queue'''
        text = "%s,%s" % (self.worker_name, redis_status)
        self.queue.put(text)

    def check_redis_server(self, timeout):
        ''' Uses the BLPOP blocking call to test that the connection is still alive.'''
        self.redis_server.blpop(REDIS_MONITOR_DUMMY_LIST, timeout)

    def redis_ready(self, redis_info):
        ''' Checks for the two propeties: "loading" and "master_sync_in_progress".
            If they are present and not zero, then the redis node is still synching with master'''
        return redis_info.get("loading", 0) == 0 and redis_info.get("master_sync_in_progress", 0) == 0

    def worker(self, redis_server, worker_name, redis_host, redis_port, sleep_time):
        ''' This is the main function, executed by each monitoring process.'''
        logger.info("%s started.", worker_name)
        while 1:
            try:
                self.check_redis_server(timeout=1)
                redis_info = redis_server.info()
                role = redis_info['role']
                if self.redis_ready(redis_info):
                    # The connection is fine, send the OK message
                    self.send_message(REDIS_STATUS_OK)                
                    logger.debug("%s: (%s) Node status [%r]", worker_name, role, REDIS_STATUS_OK)
                    self.check_redis_server(timeout=sleep_time)
                else:
                    logger.warn("%s: Redis node (%s) is not ready yet: loading: [%r], master_sync_in_progress: [%r]",
                        worker_name, role, redis_info.get("loading", ''), redis_info.get("master_sync_in_progress", ''))
            except ConnectionError, connerr:
                # Unable to connect to Redis, send the KO message
                self.send_message(REDIS_STATUS_KO)
                logger.error("%s: Redis Connection Error: %r", worker_name, connerr)
                sleep(10)
