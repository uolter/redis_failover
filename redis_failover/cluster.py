'''
@author: mturatti
''' 

from utils import \
    ROLE_MASTER, ROLE_SLAVE, \
    REDIS_STATUS_OK, REDIS_STATUS_KO


class Cluster(object):
    ''' Represents a cluster of servers'''

    def __init__(self):
        self._map = {}

    def add_node(self, host, port, role=ROLE_SLAVE, status=REDIS_STATUS_OK):
        key = self._make_key(host, port)
        node = Node(host, port, role, status)
        self._map[key] = node
        return node

    def get_master(self):
        for k in self._map:
            node = self._map[k]
            if node.is_master():
                return node
        return None

    def promote_new_master(self, old_master):
        old_master.set_slave()
        old_master.setKO()
        for k in self._map:
            node = self._map[k]
            if node.is_alive():
                self._set_role(node.host, node.port, ROLE_MASTER)
                return node
        old_master.set_master()
        return old_master

    def filtered_list(self, roles=(ROLE_MASTER, ROLE_SLAVE), status=(REDIS_STATUS_OK, REDIS_STATUS_KO)):
        return_list = []
        for k in self._map:
            node = self._map[k]
            if node.status in status and node.role in roles:
                return_list.append(node)
        return return_list

    def get_node(self, host, port):
        key = self._make_key(host, port)
        return self._map[key]

    def __len__(self):
        return len(self._map)

    def _set_role(self, host, port, role):
        key = self._make_key(host, port)
        self._map[key]._role = role

    def _make_key(self, host, port):
        return "%s:%s" % (host, port)

    def __str__(self):
        return ",".join([str(k) for k in self._map.values()])


class Node(object):
    ''' Represents a single server.'''
    
    def __init__(self, host, port, role, status):
        self._host = host
        self._port = int(port)
        self._role = role
        self._status = status

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port
    
    @property
    def role(self):
        return self._role

    @property
    def status(self):
        return self._status

    def is_master(self):
        return self._role == ROLE_MASTER

    def is_slave(self):
        return self._role == ROLE_SLAVE

    def is_alive(self):
        return self._status == REDIS_STATUS_OK

    def setOK(self):
        self._status = REDIS_STATUS_OK

    def setKO(self):
        self._status = REDIS_STATUS_KO

    def set_master(self):
        self._role = ROLE_MASTER

    def set_slave(self):
        self._role = ROLE_SLAVE

    def __str__(self):
        return "(%s:%d,%s,%s)" % (self.host, self.port, self.role, self.status)

