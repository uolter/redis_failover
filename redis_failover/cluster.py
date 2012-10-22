# cluster.py

from utils import *

class Server():
    
    def __init__(self, host, port, role, status):
        self.host = host
        self.port = int(port)
        self.role = role
        self.status = status


class Cluster:

    def __init__(self):
        self.map = {}

    def add_node(self, host, port, role=ROLE_SLAVE, status=REDIS_STATUS_OK):
        key = self.__getkey__(host, port)
        self.map[key] = Server(host, port, role, status)


    def get_master(self):
        for k in self.map:
            server = self.map[k]
            if server.role == ROLE_MASTER:
                return server


    def set_master(self, host, port):
        key = self.__getkey__(host, port)
        for k in self.map:
            server = self.map[k]
            server.role = ROLE_SLAVE
        self.__set_role__(host, port, ROLE_MASTER)


    def promote_new_master(self, old_master):
        old_master.status = REDIS_STATUS_KO
        for k in self.map:
            server = self.map[k]
            if server.role == ROLE_SLAVE and server.status == REDIS_STATUS_OK:
                self.set_master(server.host, server.port)
                return server


    def filtered_list(self, roles=(ROLE_MASTER, ROLE_SLAVE), status=(REDIS_STATUS_OK, REDIS_STATUS_KO)):
        return_list = []
        for k in self.map:
            server = self.map[k]
            if server.status in status and server.role in roles:
                return_list.append(server)
        return return_list


    def get_server(self, host, port):
        key = self.__getkey__(host, port)
        return self.map[key]


    def clear(self):
        del(self.map)
        self.map= {}


    def __set_role__(self, host, port, role):
        key =self.__getkey__(host, port)
        self.map[key].role = role


    def __getkey__(self, host, port):
        return "%s:%s" % (host, port)
