"""PGSync RedisQueue."""
import json
import logging
import time

from redis import Redis
from redis.exceptions import ConnectionError

from .settings import REDIS_CHUNK_SIZE, REDIS_SOCKET_TIMEOUT
from .utils import get_redis_url

logger = logging.getLogger(__name__)


class RedisQueue(object):
    """Simple Queue with Redis Backend."""

    def __init__(self, name, namespace="queue", **kwargs):
        """The default connection parameters are:

        host = 'localhost', port = 6379, db = 0
        """
        url = get_redis_url(**kwargs)
        self.key = f"{namespace}:{name}"
        try:
            self.__db = Redis.from_url(
                url,
                socket_timeout=REDIS_SOCKET_TIMEOUT,
            )
            self.__db.ping()
        except ConnectionError as e:
            logger.exception(f"Redis server is not running: {e}")
            raise

    def qsize(self):
        """Return the approximate size of the queue."""
        return self.__db.zcount(self.key,'-inf','+inf')

    def empty(self):
        """Return True if the queue is empty, False otherwise."""
        return self.qsize() == 0

    def push(self, item, txn_id):
        """Push item into the queue."""
        payload = json.dumps(item)
        self.__db.zadd(self.key, {payload:txn_id}, nx=True)


    def pop(self, block=True, timeout=None):
        """Remove and return an item from the queue.

        If optional args block is true and timeout is None (the default), block
        if necessary until an item is available.
        """
        if block:
            item = self.__db.bzpopmin(self.key)
        else:
            item = self.__db.zpopmin(self.key)

        if item:
            payload = json.loads(item[1])
            score = item[2]
            return (score,payload)
        else:
            return None

    def bulk_pop(self, chunk_size=None):
        """Remove and return multiple items from the queue."""
        chunk_size = chunk_size or REDIS_CHUNK_SIZE
        items = []
        while self.empty() == False:
            if len(items) > chunk_size:
                break

            # For now just use pop. I think there's a better, bulk way to do this in redis
            items.append(self.pop())
        return items

    def bulk_push(self, items):
        """Push multiple items onto the queue.
        Takes an array of tuples. First element = txn_id, second = payload"""
        for item in items:
            self.push(item[1],item[0])
        

    def pop_nowait(self):
        """Equivalent to pop(False)."""
        return self.pop(False)

    def _delete(self):
        logger.info(f"Deleting redis key: {self.key}")
        self.__db.delete(self.key)


def redis_engine(scheme=None, host=None, password=None, port=None, db=None):
    url = get_redis_url(
        scheme=scheme, host=host, password=password, port=port, db=db
    )
    try:
        conn = Redis.from_url(
            url,
            socket_timeout=REDIS_SOCKET_TIMEOUT,
        )
        conn.ping()
    except ConnectionError as e:
        logger.exception(f"Redis server is not running: {e}")
        raise
    return conn
