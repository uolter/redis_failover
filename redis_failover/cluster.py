# cluster.py

from utils import *

class Node():
    
    def __init__(self, host, port, role, status):
        self.host = host
        self.port = int(port)
        self.role = role
        self.status = status

    def is_master(self):
        return self.role == ROLE_MASTER

    def is_slave(self):
        return self.role == ROLE_SLAVE

    def is_alive(self):
        return self.status == REDIS_STATUS_OK

    def setOK(self):
        self.status = REDIS_STATUS_OK

    def setKO(self):
        self.status = REDIS_STATUS_KO

    def set_master(self):
        self.role = ROLE_MASTER

    def set_slave(self):
        self.role = ROLE_SLAVE


class Cluster:

    def __init__(self):
        self._map = {}

    def add_node(self, host, port, role=ROLE_SLAVE, status=REDIS_STATUS_OK):
        key = self._getkey(host, port)
        node = Node(host, port, role, status)
        self._map[key] = node
        return node


    def get_master(self):
        for k in self._map:
            node = self._map[k]
            if node.is_master():
                return node


    def set_master(self, host, port):
        key = self._getkey(host, port)
        for k in self._map:
            node = self._map[k]
            node.set_slave()
        self._set_role(host, port, ROLE_MASTER)


    def promote_new_master(self, old_master):
        old_master.setKO()
        for k in self._map:
            node = self._map[k]
            if node.is_alive():
                self.set_master(node.host, node.port)
                return node


    def filtered_list(self, roles=(ROLE_MASTER, ROLE_SLAVE), status=(REDIS_STATUS_OK, REDIS_STATUS_KO)):
        return_list = []
        for k in self._map:
            node = self._map[k]
            if node.status in status and node.role in roles:
                return_list.append(node)
        return return_list


    def get_node(self, host, port):
        key = self._getkey(host, port)
        return self._map[key]


    def size(self):
        return len(self._map)


    def _set_role(self, host, port, role):
        key = self._getkey(host, port)
        self._map[key].role = role


    def _getkey(self, host, port):
        return "%s:%s" % (host, port)
