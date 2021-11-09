"""RedisQueues tests."""
import pytest
from pgsync.redisqueue import RedisQueue


@pytest.mark.usefixtures("table_creator")
class TestRedisQueue(object):
    """Redis Queue tests."""
    redis_queue = RedisQueue(name="test",namespace="testns")

    redis_queue._delete()

    dummy_value = {}
    dummy_value['a'] = 1
    dummy_value['b'] = 2
    dummy_value['c'] = 3
    dummy_value['f'] = 22
    redis_queue.push(dummy_value, 1)

    assert(redis_queue.qsize() == 1)

    dv_result = redis_queue.pop()[1]
    assert(dv_result.get('b') == 2)
    assert(dv_result.get('a') == 1)
    assert(dv_result.get('d') == None)

    assert(redis_queue.empty())

    dummy_value2 = {}
    dummy_value2['e'] = 34
    redis_queue.bulk_push([(1,dummy_value), (2,dummy_value2)])

    assert(redis_queue.qsize() == 2)

    items = redis_queue.bulk_pop()
    assert(len(items) == 2)

    # Check bulk payloads
    assert(items[0][1]['f'] == 22)
    assert(items[1][1]['e'] == 34)

    # Check score
    assert(items[1][0] == 2)
    assert(redis_queue.empty())

    redis_queue = RedisQueue(name="test", namespace="testns")

    redis_queue._delete()

    dummy_value = {'a': 1, 'b': 2, 'c': 3, 'f': 22}
    redis_queue.push(dummy_value, 1)

    assert(redis_queue.qsize() == 1)

    dv_result = redis_queue.pop()[1]
    assert(dv_result.get('b') == 2)
    assert(dv_result.get('a') == 1)
    assert(dv_result.get('d') is None)

    assert(redis_queue.empty())

    dummy_value2 = {'e': 34}
    redis_queue.bulk_push([(1, dummy_value), (2, dummy_value2)])

    assert(redis_queue.qsize() == 2)

    items = redis_queue.bulk_pop()
    assert(len(items) == 2)

    # Check bulk payloads
    assert(items[0][1]['f'] == 22)
    assert(items[1][1]['e'] == 34)

    # Check score
    assert(items[1][0] == 2)
    assert(redis_queue.empty())

    pass
