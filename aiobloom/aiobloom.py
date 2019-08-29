import aioredis
import math
import hashlib
from struct import unpack, pack

# modify from https://github.com/jaybaird/python-bloomfilter/blob/master/pybloom/pybloom.py

import sys
try:
    import StringIO
    import cStringIO
except ImportError:
    from io import BytesIO

running_python_3 = sys.version_info[0] == 3


def range_fn(*args):
    return range(*args)


def is_string_io(instance):
    return isinstance(instance, BytesIO)


def make_hashfuncs(num_slices, num_bits):
    if num_bits >= (1 << 31):
        fmt_code, chunk_size = 'Q', 8
    elif num_bits >= (1 << 15):
        fmt_code, chunk_size = 'I', 4
    else:
        fmt_code, chunk_size = 'H', 2
    total_hash_bits = 8 * num_slices * chunk_size
    if total_hash_bits > 384:
        hashfn = hashlib.sha512
    elif total_hash_bits > 256:
        hashfn = hashlib.sha384
    elif total_hash_bits > 160:
        hashfn = hashlib.sha256
    elif total_hash_bits > 128:
        hashfn = hashlib.sha1
    else:
        hashfn = hashlib.md5
    fmt = fmt_code * (hashfn().digest_size // chunk_size)
    num_salts, extra = divmod(num_slices, len(fmt))
    if extra:
        num_salts += 1
    salts = tuple(hashfn(hashfn(pack('I', i)).digest()) for i in range_fn(num_salts))
    def _make_hashfuncs(key):

        if isinstance(key, str):
            key = key.encode('utf-8')
        else:
            key = str(key).encode('utf-8')

        i = 0
        for salt in salts:
            h = salt.copy()
            h.update(key)
            for uint in unpack(fmt, h.digest()):
                yield uint % num_bits
                i += 1
                if i >= num_slices:
                    return

    return _make_hashfuncs


class BloomFilter(object):


    def __init__(self, capacity, error_rate=0.001, bloom_key='bloom_key',
            redis_pool=None):

        if not (0 < error_rate < 1):
            raise ValueError("Error_Rate must be between 0 and 1.")
        if not capacity > 0:
            raise ValueError("Capacity must be > 0")
        # given M = num_bits, k = num_slices, P = error_rate, n = capacity
        #       k = log2(1/P)
        # solving for m = bits_per_slice
        # n ~= M * ((ln(2) ** 2) / abs(ln(P)))
        # n ~= (k * m) * ((ln(2) ** 2) / abs(ln(P)))
        # m ~= n * abs(ln(P)) / (k * (ln(2) ** 2))
        num_slices = int(math.ceil(math.log(1.0 / error_rate, 2)))
        bits_per_slice = int(math.ceil(
            (capacity * abs(math.log(error_rate))) /
            (num_slices * (math.log(2) ** 2))))
        self._setup(error_rate, num_slices, bits_per_slice, capacity, 0)
        self.bloom_key = bloom_key
        self.pool = redis_pool


    def _setup(self, error_rate, num_slices, bits_per_slice, capacity, count):
        self.error_rate = error_rate
        self.num_slices = num_slices
        self.bits_per_slice = bits_per_slice
        self.capacity = capacity
        self.num_bits = num_slices * bits_per_slice
        self.count = count
        self.make_hashes = make_hashfuncs(self.num_slices, self.bits_per_slice)

    async def connect(self, redis_url='127.0.0.1:6379'):
        if self.pool:
            return

        host, _, port = redis_url.partition(':')
        if not port:
            port = 6379
        try:
            port = int(port)
        except ValueError:
            raise ValueError(port)
        self.pool = await aioredis.create_redis_pool(
            (host, port), minsize=10, maxsize=60)
        print("redis_pool", self.pool, id(self.pool))

    async def exist(self, key):

        hashes = self.make_hashes(key)
        hashes = list(hashes)
        offset = 0
        with await self.pool as redis:
            bloom_filter = await redis.get(self.bloom_key)
            include = True
            for hash_position in hashes:
                index = offset + hash_position
                hash_byte_index = index // 8
                if hash_byte_index >= len(bloom_filter):
                    # the hash key is big then total it should not exist:
                    return False
                bit_is_set = bloom_filter[hash_byte_index] >> (7 - index % 8) & 1
                if not bit_is_set:
                    include = False
                    break
                offset += self.bits_per_slice
        return include

    async def add(self, key, skip_check=False):
        hashes = self.make_hashes(key)
        if self.count > self.capacity:
            raise IndexError("BloomFilter is at capacity")

        offset = 0
        with await self.pool as redis:
            pipe = redis.pipeline()
            for hash_position in hashes:
                index = offset + hash_position
                pipe.setbit(self.bloom_key, index, 1)
                offset += self.bits_per_slice
            await pipe.execute()

    def __getstate__(self):
        d = self.__dict__.copy()
        del d['make_hashes']
        return d

    def __setstate__(self, d):
        self.__dict__.update(d)
        self.make_hashes = make_hashfuncs(self.num_slices, self.bits_per_slice)
