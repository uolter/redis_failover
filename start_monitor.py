'''
@author: mturatti
'''

import logging
import logging.config
logging.config.fileConfig("loggers_redis_monitor.conf")
logger = logging.getLogger("redis_monitor")

from zktools.locking import ZkLock
from zc.zk import ZooKeeper, FailedConnect
from optparse import OptionParser
from redis_failover.redis_monitor import RedisMonitor

ZOOKEEPER_LOCK = 'redis_monitor_lock'
LOG_FILE = None

def opt_callback(option, opt, value, parser):
    setattr(parser.values, option.dest, value.split(','))

def main():
    logger.info("Enter")
    SLEEP_TIME = 300
    parser = OptionParser()
    parser.add_option("-z", "--zkhosts", dest="zk_hosts", type='string', action='callback', callback=opt_callback,
                  help="zookeeper list of host:port comma separated ")
    parser.add_option("-p", "--path", dest="zk_path", type='string',
                  help="zookeeper root path")
    parser.add_option("-r", "--redishosts", dest="rs_hosts",type='string', action='callback', callback=opt_callback,
                  help="redis list of host:port comma separated ")
    parser.add_option("-s", "--sleeptime", dest="sleep_time", type='int', help="waiting time in seconds between thread execution")
    options = parser.parse_args()[0]
    if options.sleep_time:
        SLEEP_TIME = options.sleep_time
        logger.info("SLEEP_TIME: %d", SLEEP_TIME)
    logger.info("Options: %r", options)
    logger.info("Acquiring a exclusive lock on ZooKeeper...")
    try:
        zk = ZooKeeper(','.join(options.zk_hosts))
        with ZkLock(zk, ZOOKEEPER_LOCK):
            logger.info("Lock acquired! Connecting to Redis...")
            nm = RedisMonitor(zk, options.rs_hosts, SLEEP_TIME, options.zk_path)
            nm.execute()
    except FailedConnect, err:
        logger.critical(err)
    logger.info("Exit")


if __name__ == '__main__':
    main()
