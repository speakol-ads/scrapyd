from zope.interface import implementer

from scrapyd.interfaces import ISpiderQueue
from scrapyd.sqlite import JsonSqlitePriorityQueue
import logging

@implementer(ISpiderQueue)
class SqliteSpiderQueue(object):

    def __init__(self, database=None, table='spider_queue'):
        self.q = JsonSqlitePriorityQueue(database, table)

    def add(self, name, priority=0.0, **spider_args):
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
        self.q.put(d, priority=priority)
        logging.debug(len(self.q))
        logging.debug("//#################")
        logging.debug("//#################")
        logging.debug("//#################")

    def pop(self):
        ret = self.q.pop()
        logging.debug("//#################")
        logging.debug(ret)
        logging.debug("//#################")
        return ret

    def count(self):
        return len(self.q)

    def list(self):
        return [x[0] for x in self.q]

    def remove(self, func):
        return self.q.remove(func)

    def clear(self):
        self.q.clear()
