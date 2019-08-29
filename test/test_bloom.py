
import pytest
import aioredis
import unittest

from aiobloom.aiobloom import (
    BloomFilter, make_hashfuncs, range_fn, running_python_3)

try:
    import StringIO
    import cStringIO
except ImportError:
    pass

import io

@pytest.fixture(scope='function')
async def redis_pool():
    pool = await aioredis.create_redis_pool(
            ("127.0.0.1", 6379), minsize=10, maxsize=60)
    yield pool
    pool.close()
    print("after use pool")



@pytest.mark.asyncio()
async def test_bloom_in_should_in(redis_pool):
    with await redis_pool as redis:
        key = "test_bloom"
        await redis.delete(key)

        bloom_one = BloomFilter(100000, 0.1, bloom_key=key, redis_pool=redis_pool)
        chars = [str(i) for i in range_fn(10000)]
        real_in = chars[:1000]
        real_not_in = chars[1000:]
        bloom_in = []
        bloom_not_in = []
        for char in chars[:1000]:
            await bloom_one.add(char)
        for char in chars:
            isin = await bloom_one.exist(char)
            if isin:
                bloom_in.append(char)
            else:
                bloom_not_in.append((char))

        print("real_in", real_in)
        print("bloom_in", bloom_in)
        assert set(bloom_in) - set(real_in)  == set ()


@pytest.mark.asyncio
async def test_bloom_in_may_not_in(redis_pool):
    with await redis_pool as redis:
        key = "test_bloom_1"
        await redis.delete(key)
        total = 10000
        error_rate = 0.01
        bloom_one = BloomFilter(total, error_rate, bloom_key=key, redis_pool=redis_pool)
        chars = [str(i) for i in range_fn(total)]

        put_num = 1000
        bloom_in = []
        bloom_not_in = []
        for char in chars[:put_num]:
            await bloom_one.add(char)
        for char in chars[put_num:]:
            isin = await bloom_one.exist(char)
            if isin:
                bloom_in.append(char)
            else:
                bloom_not_in.append(char)

        # print("real_not_in", real_not_in)
        # print("bloom_not", bloom_not_in)
        print("bloom_in", bloom_in)
        print("bloom in len ", len(bloom_in))
        print("bloom_not_in", len(bloom_not_in))
        rate = len(bloom_in) / total
        print("false rate", )
        assert rate < error_rate


if __name__ == '__main__':
    unittest.main()