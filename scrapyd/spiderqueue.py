from zope.interface import implementer

from scrapyd.interfaces import ISpiderQueue
from scrapyd.sqlite import JsonSqlitePriorityQueue
import logging

@implementer(ISpiderQueue)
class SqliteSpiderQueue(object):

    def __init__(self, database=None, table='spider_queue'):
        self.q = JsonSqlitePriorityQueue(database, table)
        self.q2 = []

    def add(self, name, priority=0.0, **spider_args):
        # self.q2.append()
        logging.debug("#####################")
        logging.debug("#####################")
        logging.debug("#####################")
        logging.debug("#####################")
        logging.debug("#####################")
        logging.debug("#####################")
        logging.debug("#####################")
        logging.debug("#####################")
        d = spider_args.copy()
        d['name'] = name
        # self.q.put(d, priority=priority)
        self.q2.append(d)
        logging.debug(len(self.q))
        logging.debug("//#################")
        logging.debug("//#################")
        logging.debug("//#################")

    def pop(self):
        return self.q2.pop()

        ret = self.q.pop()
        logging.debug("//#################")
        logging.debug(ret)
        logging.debug("//#################")
        return ret

    def count(self):
        return len(self.q2)

    def list(self):
        return [x for x in self.q2]

    def remove(self, func):
        return 1
        return self.q.remove(func)

    def clear(self):
        self.q2 = []
        self.q.clear()
