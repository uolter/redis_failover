from multiprocessing import Process, Queue
from time import sleep

import redis
from redis.exceptions import ConnectionError
import logging

from zktools.locking import ZkLock
from zc.zk import *
from redis_failover.utils import *

from cluster import Cluster

class Constants():
    
    REDIS_MONITOR_DUMMY_LIST = 'redis_monitor_dummy_list'
    SLEEP_TIME = -1
    ZOOKEEPER_LOCK = 'redis_monitor_lock'

class RedisMonitor():

    def __init__(self, zk, redis_hosts, zk_path='/redis/cluster'):
        self.zk = zk
        self.cluster = Cluster()
        self.queue = Queue()
        self.redis_hosts = redis_hosts
        self.zk_properties = None
        self.list_of_workers = []
        self.zk_path = zk_path


    def discover_redis(self):
        '''
            check all the redis instances and registers to zk host, port and status.
            It also prepares all the worker processes for monitoring.
        '''
        logger.info('Enter')
        self.zk.create_recursive(self.zk_path, '', OPEN_ACL_UNSAFE)
        self.zk_properties = self.zk.properties(self.zk_path)

        for i, r in enumerate(self.redis_hosts):
            role=ROLE_SLAVE
            host = r.split(':')[0]
            port = int(r.split(':')[1])
            try:
                role = get_redis_node(host=host, port=port).info()['role']
                self.cluster.add_node(host, port, role, REDIS_STATUS_OK)
            except ConnectionError:
                logger.error("Server [%r] not available !!", r)
                self.cluster.add_node(host, port, role, REDIS_STATUS_KO)
            finally:
                self.list_of_workers.append(Worker(host, port, self.queue))

        node_master = self.cluster.get_master()
        master_node = ('%s:%s' %(node_master.host, node_master.port), REDIS_STATUS_OK)
        logger.info('master: [%r]', master_node[0])

        node_slaves = self.cluster.filtered_list((ROLE_SLAVE, ))
        slaves_map = {}
        for i, slave in enumerate(node_slaves):
            slaves_map[i] = ('%s:%s' % (slave.host, slave.port), slave.status)

        logger.info("slaves: [%r] ", slaves_map)
        self.__update_zk_properties__(master = master_node, slaves = slaves_map)

        # start all workers
        for worker in self.list_of_workers:
            worker.start()

        logger.info("%d Workers created", len(self.list_of_workers))
        logger.info("Exit")


    def promote_new_master(self, old_master):
        logger.info("Enter")
        # set this node as the master
        new_master = self.cluster.promote_new_master(old_master)
        redis_node = get_redis_node(host=new_master.host, port=new_master.port)
        redis_node.slaveof()
        # set all the other nodes as slaves of the master
        list_of_slaves = self.cluster.filtered_list(roles=(ROLE_SLAVE,), status=(REDIS_STATUS_OK,))
        for slave in list_of_slaves:
            redis_node = get_redis_node(host=slave.host, port=slave.port)
            redis_node.slaveof(host=new_master.host, port=new_master.port)
        logger.info("Exit")


    def execute(self):
        logger.info("Enter")
        self.discover_redis()
        while True:
            message = self.queue.get()
            logger.debug("Received message from Worker: [%s]", message)
            server, new_status = message.split(',')
            host, port = server.split(':')
            redis_node = self.cluster.get_server(host, port)
            old_status = redis_node.status

            if new_status == REDIS_STATUS_KO:
                if old_status == REDIS_STATUS_OK:
                    logger.warn("Node (%s) [%s] has DIED!", redis_node.role, server)
                    if redis_node.role == ROLE_MASTER:
                        logger.warn("Master is down: promoting a new master...")
                        self.promote_new_master(redis_node)
                    else:
                        redis_node.status = REDIS_STATUS_KO
                    self.update_zk()
            elif new_status == REDIS_STATUS_OK:
                if old_status == REDIS_STATUS_KO:
                    logger.warn("Node (%s) [%s] has RESURRECTED!", redis_node.role, server)
                    redis_node.status = REDIS_STATUS_OK
                    redis_node = get_redis_node(host=redis_node.host, port=redis_node.port)
                    master = self.cluster.get_master()
                    redis_node.slaveof(master.host, master.port)
                    self.update_zk()
            else:
                logger.critical("Worker sent an unknown status: [%r]", new_status)
            
            # check if all worker daemons are still alive, if not then re-start
            for worker in self.list_of_workers:
                if not worker.get_process().is_alive():
                    logger.error("Process [%s] has died, restarting it...", worker.get_process().name)
                    worker.start()

        logger.info("Exit")


    def update_zk(self):
        logger.info("Enter")
        master = self.cluster.get_master()
        master_node = ('%s:%s' %(master.host, master.port), REDIS_STATUS_OK)
        logger.info("master_node: [%r]", master_node)
        list_of_slaves = self.cluster.filtered_list(roles=(ROLE_SLAVE,))
        slaves_map = {}
        for i, slave in enumerate(list_of_slaves):
            slaves_map[i] = ("%s:%s" % (slave.host, slave.port), slave.status)
        logger.info("slaves_map: [%r]", slaves_map)
        logger.warn("Updating ZooKeeper...")
        self.__update_zk_properties__(master = master_node, slaves = slaves_map)
        logger.info("Exit")


    def __update_zk_properties__(self, master, slaves):
       self.zk_properties.update(master = master, slaves = slaves)


class Worker():

    def __init__(self, redis_host, redis_port, queue):
        self.redis_host = redis_host
        self.redis_port = int(redis_port)
        self.queue = queue
        self.worker_name = '%s:%s' % (self.redis_host, self.redis_port)
        self.process = None

    def start(self):
        self.process = Process(target=self.worker, name=self.worker_name, 
            args=(self.worker_name, self.redis_host, self.redis_port))
        self.process.daemon = True
        self.process.start()


    def get_process(self):
        return self.process


    def send_message(self, redis_status):
        text = "%s,%s" % (self.worker_name, redis_status)
        self.queue.put(text)


    def worker(self, worker_name, redis_host, redis_port):
        logger.info("%s started.", worker_name)
        # connect to Redis
        redis_node = get_redis_node(host=redis_host, port=redis_port)
        while True:
            try:
                check_redis_node(redis_node, 1)
                redis_info = redis_node.info()
                role = redis_info['role']
                if redis_ready(redis_info):
                    # send the OK message
                    self.send_message(REDIS_STATUS_OK)                
                    logger.debug("%s: (%s) Node status [%r]", worker_name, role, REDIS_STATUS_OK)
                    check_redis_node(redis_node, Constants.SLEEP_TIME)
                else:
                    logger.warn("%s: Redis node (%s) is not ready yet: loading: [%r], master_sync_in_progress: [%r]",
                        worker_name, role, redis_info.get("loading", ''), redis_info.get("master_sync_in_progress", ''))
            except ConnectionError, connerr:
                # send the KO message
                self.send_message(REDIS_STATUS_KO)
                logger.error("%s: Redis Connection Error: %r", worker_name, connerr)
                sleep(10)

#
# Module objects
#

def redis_ready(redis_info):
    return redis_info.get("loading", 0) == 0 and redis_info.get("master_sync_in_progress", 0) == 0


def check_redis_node(redis_node, timeout):
    redis_node.blpop(Constants.REDIS_MONITOR_DUMMY_LIST, timeout)


def get_redis_node(host, port):
    return redis.Redis(host, int(port))
    

def main():
    logger.info("Enter")
    from optparse import OptionParser
    def opt_callback(option, opt, value, parser):
        setattr(parser.values, option.dest, value.split(','))

    parser = OptionParser()
    parser.add_option("-z", "--zk_hosts", dest="zk_hosts", type='string', action='callback', callback=opt_callback,
                  help="zookeeper list of host:port comma separated ")
    parser.add_option("-p", "--path", dest="zk_path", type='string',
                  help="zookeeper root path")
    parser.add_option("-r", "--redis_hosts", dest="rs_hosts",type='string', action='callback', callback=opt_callback,
                  help="redis list of host:port comma separated ")
    parser.add_option("-s", "--sleep_time", dest="sleep_time", type='int', help="waiting time in seconds between thread execution")

    options = parser.parse_args()[0]
    if options.sleep_time:
        Constants.SLEEP_TIME = options.sleep_time
        logger.info("SLEEP_TIME: %d", Constants.SLEEP_TIME)

    logger.info("Options: %r", options)

    logger.info("Acquiring a exclusive lock on ZooKeeper...")
    try:
        zk = ZooKeeper(','.join(options.zk_hosts))
        with ZkLock(zk, Constants.ZOOKEEPER_LOCK) as lock:
            logger.info("Lock acquired! Connecting to Redis...")
            nm = RedisMonitor(zk=zk, redis_hosts=options.rs_hosts, zk_path=options.zk_path)
            nm.execute()
    except FailedConnect, err:
        logger.critical(err)
    logger.info("Exit")


if __name__ == '__main__':
    # Setup loggers
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    # create formatter and add it to the handlers
    fmt = logging.Formatter('%(asctime)s - pid_%(process)d - %(levelname)s  - %(funcName)s - %(message)s')
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    # run main function
    main()
