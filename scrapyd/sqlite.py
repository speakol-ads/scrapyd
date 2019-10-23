import sqlite3
import json
try:
    from collections.abc import MutableMapping
except ImportError:
    from collections import MutableMapping
import six

import heapq
from ._deprecate import deprecate_class

import redis

#class JsonSqliteDict(dict):
#    pass

class JsonSqlitePriorityQueue(object):
    """SQLite priority queue. It relies on SQLite concurrency support for
    providing atomic inter-process operations.
    """

    def __init__(self, database=None, table="queue"):
        self.key = "spklscrapyd.pqueue"
        self.db = redis.StrictRedis(host='172.31.14.231', port=30037,password='speakol.inline.redis',
                                            charset="utf-8", decode_responses=True,
                                            db=13) 

    def put(self, message, priority=0.0):
        self.db.lpush(self.key, self.encode(message))

    def pop(self):
       return self.db.lpop(self.key)

    def remove(self, func):
        n = 0
        return n

    def clear(self):
        self.db = []

    def __len__(self):
        return len(self.db.llen(self.key))

    def __iter__(self):
        return [self.decode(v) for v in self.db.lrange(self.key, 0, -1)]

    def encode(self, obj):
        return json.dumps(obj).encode('ascii')

    def decode(self, text):
        return json.loads(bytes(text).decode('ascii'))



class JsonSqliteDict(MutableMapping):
    """SQLite-backed dictionary"""

    def __init__(self, database=None, table="dict"):
        self.key = "spklscrapyd.sqlitedict"
        self.db = redis.StrictRedis(host='172.31.14.231', port=30037, password='speakol.inline.redis',
                                            charset="utf-8", decode_responses=True,
                                            db=13) 
	#self.database = database or ':memory:'
        #self.table = table
        # about check_same_thread: http://twistedmatrix.com/trac/ticket/4040
        #self.conn = sqlite3.connect(self.database, check_same_thread=False)
        #self.conn.execute(q)
	#self.db = {}

    def __getitem__(self, key):
        return self.db.hget(self.key, self.encode(key))
        return self.db[key]
        key = self.encode(key)
        q = "select value from %s where key=?" % self.table
        value = self.conn.execute(q, (key,)).fetchone()
        if value:
            return self.decode(value[0])
        raise KeyError(key)

    def __setitem__(self, key, value):
        self.hset(self.key, self.encode(key), self.encode(value))
        return
        self.db[key] = value
        return
        key, value = self.encode(key), self.encode(value)
        q = "insert or replace into %s (key, value) values (?,?)" % self.table
        self.conn.execute(q, (key, value))
        self.conn.commit()

    def __delitem__(self, key):
        self.db.hdel(self.key, key)
        return
        del self.db[key]
        return
        key = self.encode(key)
        q = "delete from %s where key=?" % self.table
        self.conn.execute(q, (key,))
        self.conn.commit()

    def __len__(self):
        return self.db.hlen(self.key)
        return len(self.db)
        q = "select count(*) from %s" % self.table
        return self.conn.execute(q).fetchone()[0]

    def __iter__(self):
        for k in self.iterkeys():
            yield k

    def iterkeys(self):
        return self.db.hgetall(self.key).keys()

        q = "select key from %s" % self.table
        return (self.decode(x[0]) for x in self.conn.execute(q))

    def keys(self):
        return list(self.iterkeys())

    def itervalues(self):
        return  self.db.hgetall(self.key).values()
        q = "select value from %s" % self.table
        return (self.decode(x[0]) for x in self.conn.execute(q))

    def values(self):
        return list(self.itervalues())

    def iteritems(self):
        return self.db.hgetall(self.key).iteritems()
        q = "select key, value from %s" % self.table
        return ((self.decode(x[0]), self.decode(x[1])) for x in self.conn.execute(q))

    def items(self):
        return list(self.iteritems())

    def encode(self, obj):
        return json.dumps(obj).encode('ascii')

    def decode(self, obj):
        return json.loads(bytes(obj).decode('ascii'))

class JsonSqlitePriorityQueueLegacy(object):
    """SQLite priority queue. It relies on SQLite concurrency support for
    providing atomic inter-process operations.
    """

    def __init__(self, database=None, table="queue"):
        self.database = database or ':memory:'
        self.table = table
        # about check_same_thread: http://twistedmatrix.com/trac/ticket/4040
        self.conn = sqlite3.connect(self.database, check_same_thread=False)
        q = "create table if not exists %s (id integer primary key, " \
            "priority real key, message blob)" % table
        self.conn.execute(q)

    def put(self, message, priority=0.0):
        args = (priority, self.encode(message))
        q = "insert into %s (priority, message) values (?,?)" % self.table
        self.conn.execute(q, args)
        self.conn.commit()

    def pop(self):
        q = "select id, message from %s order by priority desc limit 1" \
            % self.table
        idmsg = self.conn.execute(q).fetchone()
        if idmsg is None:
            return
        id, msg = idmsg
        q = "delete from %s where id=?" % self.table
        c = self.conn.execute(q, (id,))
        if not c.rowcount: # record vanished, so let's try again
            self.conn.rollback()
            return self.pop()
        self.conn.commit()
        return self.decode(msg)

    def remove(self, func):
        q = "select id, message from %s" % self.table
        n = 0
        for id, msg in self.conn.execute(q):
            if func(self.decode(msg)):
                q = "delete from %s where id=?" % self.table
                c = self.conn.execute(q, (id,))
                if not c.rowcount: # record vanished, so let's try again
                    self.conn.rollback()
                    return self.remove(func)
                n += 1
        self.conn.commit()
        return n

    def clear(self):
        self.conn.execute("delete from %s" % self.table)
        self.conn.commit()

    def __len__(self):
        q = "select count(*) from %s" % self.table
        return self.conn.execute(q).fetchone()[0]

    def __iter__(self):
        q = "select message, priority from %s order by priority desc" % \
            self.table
        return ((self.decode(x), y) for x, y in self.conn.execute(q))

    def encode(self, obj):
        return sqlite3.Binary(json.dumps(obj).encode('ascii'))

    def decode(self, text):
        return json.loads(bytes(text).decode('ascii'))
