# -*- encoding: utf-8 -*-
from __future__ import absolute_import
import math
import hashlib
from utils import range_fn, is_string_io, running_python_3
from struct import unpack, pack, calcsize

# MAX offset argument of redis bit operator is required to be greater than or equal to 0, and smaller than 2^32 (this limits bitmaps to 512MB). When the string at key is grown, added bits are set to 0
MAX_PER_SLICE_SIZE = 4294967295


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
        if running_python_3:
            if isinstance(key, str):
                key = key.encode('utf-8')
            else:
                key = str(key).encode('utf-8')
        else:
            if isinstance(key, unicode):
                key = key.encode('utf-8')
            else:
                key = str(key)
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


class RedisBloomFilter(object):
    REDIS_BF_SLICE_KEY_FMT = "%s:bf:s:%s"
    REDIS_BF_META_HASH_KEY = "%s:bf:meta"
    REDIS_BF_HASH_FIELD_CONF = "conf"
    REDIS_BF_HASH_FIELD_COUNT = "count"
    REDIS_CNF_FMT = b'<dQQQ'

    def __init__(self, server, bfkeypreffix, capacity, error_rate=0.001):
        """Implements a space-efficient probabilistic data structure
        server : Redis
            The redis server instance.    
        bfkeypreffix : String
            Redis key preffix.    
        capacity
            this BloomFilter must be able to store at least *capacity* elements
            while maintaining no more than *error_rate* chance of false
            positives
        error_rate
            the error_rate of the filter returning false positives. This
            determines the filters capacity. Inserting more than capacity
            elements greatly increases the chance of false positives.

        >>> b = RedisBloomFilter(server=server, bfkeypreffix="atest:bf", capacity=100000, error_rate=0.001)
        >>> b.add("test")
        False
        >>> "test" in b
        True

        """
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
        if bits_per_slice > MAX_PER_SLICE_SIZE:
            raise ValueError("Capacity[%s] and error_rate[%s] make per slice size extended, MAX_PER_SLICE_SIZE is %s, now is %s" % (capacity, error_rate, MAX_PER_SLICE_SIZE, bits_per_slice))
        self._setup(error_rate, num_slices, bits_per_slice, capacity, server, bfkeypreffix)

    def _setup(self, error_rate, num_slices, bits_per_slice, capacity, server, bfkeypreffix):
        self.error_rate = error_rate
        self.num_slices = num_slices
        self.bits_per_slice = bits_per_slice
        self.capacity = capacity
        self.num_bits = num_slices * bits_per_slice
        self.make_hashes = make_hashfuncs(self.num_slices, self.bits_per_slice)
        self.bfkeypreffix = bfkeypreffix
        self.server = server
        self.sliceKeys = tuple(self.REDIS_BF_SLICE_KEY_FMT % (self.bfkeypreffix, i) for i in range(num_slices))
        self.bfMetaKey = self.REDIS_BF_META_HASH_KEY % self.bfkeypreffix
        self._checkExists()
        

    def _checkExists(self):
        existsCnf = self.server.hget(self.bfMetaKey, self.REDIS_BF_HASH_FIELD_CONF)
        if not existsCnf:
            self.server.hset(self.bfMetaKey, self.REDIS_BF_HASH_FIELD_CONF, pack(self.REDIS_CNF_FMT, self.error_rate, self.num_slices,
                     self.bits_per_slice, self.capacity))
            pipe = self.server.pipeline(transaction=True)
            for key in self.sliceKeys:
                pipe.delete(key)
            pipe.hset(self.bfMetaKey, self.REDIS_BF_HASH_FIELD_COUNT, 0)
            pipe.execute()
        else:
            error_rate, num_slices, bits_per_slice, capacity = unpack(self.REDIS_CNF_FMT, existsCnf)
            if self.error_rate != error_rate or self.num_slices != num_slices or self.bits_per_slice != bits_per_slice or self.capacity != capacity:
                raise ValueError("setup configure not match exists")


    def __contains__(self, key):
        """Tests a key's membership in this bloom filter.

        >>> b = RedisBloomFilter(server=server, bfkeypreffix="atest:bf", capacity=100000, error_rate=0.001)
        >>> b.add("hello")
        False
        >>> "hello" in b
        True

        """
        hashes = self.make_hashes(key)
        pipe = self.server.pipeline(transaction=False) 
        sliceIdx = 0
        for k in hashes:
            sliceKey = self.REDIS_BF_SLICE_KEY_FMT % (self.bfkeypreffix, sliceIdx)
            pipe.getbit(sliceKey, k)
            sliceIdx += 1
        getbits = pipe.execute()  
        for bit in getbits:
            if not bit:
                return False
        return True

    def __len__(self):
        """Return the number of keys stored by this bloom filter."""
        count = self.server.hget(self.bfMetaKey, self.REDIS_BF_HASH_FIELD_COUNT)
        if count:
            return int(count)
        return 0

    @property
    def count(self):
        return len(self)

    def add(self, key, skip_check=False):
        """ Adds a key to this bloom filter. If the key already exists in this
        filter it will return True. Otherwise False.

        >>> b = RedisBloomFilter(server=server, bfkeypreffix="atest:bf", capacity=100000, error_rate=0.001)
        >>> b.add("hello")
        False
        >>> b.add("hello")
        True
        >>> len(b)
        1

        """
        hashes = self.make_hashes(key)
        found_all_bits = True
        if len(self) >= self.capacity:
            raise IndexError("RedisBloomFilter is at capacity")
        # TODO, check and increase not async
        pipe = self.server.pipeline(transaction=False) 
        sliceIdx = 0
        for k in hashes:
            sliceKey = self.REDIS_BF_SLICE_KEY_FMT % (self.bfkeypreffix, sliceIdx)
            pipe.setbit(sliceKey, k, 1)
            sliceIdx += 1
        pipeResults = pipe.execute()
        if not skip_check:
            for pipeResult in pipeResults:
                if not pipeResult:
                    found_all_bits = False
                    break
        if skip_check:
            self.server.hincrby(self.bfMetaKey, self.REDIS_BF_HASH_FIELD_COUNT, 1)
            return False
        elif not found_all_bits:
            self.server.hincrby(self.bfMetaKey, self.REDIS_BF_HASH_FIELD_COUNT, 1)
            return False
        else:
            return True

    def clear(self):
        pipe = self.server.pipeline(transaction=True)
        pipe.delete(self.bfMetaKey)
        for key in self.sliceKeys:
            pipe.delete(key)
        pipe.execute()
        del self.sliceKeys[:]

    def copy(self):
        """Return a copy of this bloom filter.
        """
        raise NotImplementedError("RedisBloomFilter not support copy")

    def union(self, other):
        """ Calculates the union of the two underlying bitarrays and returns
        a new bloom filter object."""
        if self.capacity != other.capacity or self.error_rate != other.error_rate:
            raise ValueError("Unioning filters requires both filters to have \
both the same capacity and error rate")
        raise NotImplementedError("RedisBloomFilter not support union")

    def __or__(self, other):
        raise NotImplementedError("RedisBloomFilter not support or")

    def intersection(self, other):
        """ Calculates the intersection of the two underlying bitarrays and returns
        a new bloom filter object."""
        if self.capacity != other.capacity or self.error_rate != other.error_rate:
            raise ValueError("Intersecting filters requires both filters to \
have equal capacity and error rate")
        raise NotImplementedError("RedisBloomFilter not support intersection")

    def __and__(self, other):
        raise NotImplementedError("RedisBloomFilter not support and")

    def tofile(self, f):
        """Write the bloom filter to file object `f'. Underlying bits
        are written as machine values. This is much more space
        efficient than pickling the object."""
        raise NotImplementedError("RedisBloomFilter not support tofile")

    @classmethod
    def fromfile(cls, f, n=-1):
        """Read a bloom filter from file-object `f' serialized with
        ``BloomFilter.tofile''. If `n' > 0 read only so many bytes."""
        raise NotImplementedError("RedisBloomFilter not support fromfile")

    def __getstate__(self):
        d = self.__dict__.copy()
        del d['make_hashes']
        return d

    def __setstate__(self, d):
        self.__dict__.update(d)
        self.make_hashes = make_hashfuncs(self.num_slices, self.bits_per_slice)


class ScalableRedisBloomFilter(object):
    SMALL_SET_GROWTH = 2 # slower, but takes up less memory
    LARGE_SET_GROWTH = 4 # faster, but takes up more memory faster
    REDIS_SBF_FILTERS_KEY_FMT = '%s:sbf:fs'
    REDIS_EACH_FILTER_KEY_FMT = '%s:sbf:f:%s'
    REDIS_SBF_META_HASH_KEY = "%s:sbf:meta"
    REDIS_SBF_META_CONF_HASH_FIELD = "conf"
    # REDIS_SBF_META_COUNT_HASH_FIELD = "count"
    REDIS_SBF_META_FILTER_IDX_HASH_FIELD = "f-id"
    REDIS_SBF_META_FILTERS_LOCK_HASH_FIELD = "lock"
    REDIS_CNF_FMT = '<idQddi'

    def __init__(self, server, bfkeypreffix, initial_capacity=100, error_rate=0.001,
                 max_filters = -1,
                 mode=SMALL_SET_GROWTH):
        """Implements a space-efficient probabilistic data structure that
        grows as more items are added while maintaining a steady false
        positive rate

        server : Redis
            The redis server instance.  
        bfkeypreffix : String
            Redis key preffix.    
        initial_capacity
            the initial capacity of the filter
        error_rate
            the error_rate of the filter returning false positives. This
            determines the filters capacity. Going over capacity greatly
            increases the chance of false positives.
        max_filters : int
            max filters hold after growth. if max_filters < 0, infinite.
        mode
            can be either ScalableBloomFilter.SMALL_SET_GROWTH or
            ScalableBloomFilter.LARGE_SET_GROWTH. SMALL_SET_GROWTH is slower
            but uses less memory. LARGE_SET_GROWTH is faster but consumes
            memory faster.

        >>> b = ScalableRedisBloomFilter(server=server, bfkeypreffix="satest:bf", initial_capacity=512, error_rate=0.001, \
                                    max_filters = 5,
                                    mode=ScalableRedisBloomFilter.SMALL_SET_GROWTH)
        >>> b.add("test")
        False
        >>> "test" in b
        True
        >>> unicode_string = u'¡'
        >>> b.add(unicode_string)
        False
        >>> unicode_string in b
        True
        """
        if not error_rate or error_rate < 0:
            raise ValueError("Error_Rate must be a decimal less than 0.")
        self._setup(mode, 0.9, initial_capacity, error_rate, server, bfkeypreffix, max_filters)

    def _setup(self, mode, ratio, initial_capacity, error_rate, server, bfkeypreffix, max_filters):
        self.scale = mode
        self.ratio = ratio
        self.initial_capacity = initial_capacity
        self.error_rate = error_rate
        self.filterErrorRate = self.error_rate * (1.0 - self.ratio)
        num_slices = int(math.ceil(math.log(1.0 / self.filterErrorRate, 2)))
        self.MAX_FILTER_CAPACITY = int(math.floor(MAX_PER_SLICE_SIZE * (num_slices * (math.log(2) ** 2)) / abs(math.log(self.filterErrorRate))))
        print "MAX FILTER Capacity", self.MAX_FILTER_CAPACITY
        self.server = server
        self.bfkeypreffix = bfkeypreffix
        self.max_filters = max_filters
        self.sbfMetaKey = self.REDIS_SBF_META_HASH_KEY % self.bfkeypreffix
        self.sbfFilterKeys = self.REDIS_SBF_FILTERS_KEY_FMT % (self.bfkeypreffix)
        self._checkExists()
        self.filtersMap = {}

    def _checkExists(self):
        existsCnf = self.server.hget(self.sbfMetaKey, self.REDIS_SBF_META_CONF_HASH_FIELD)
        if not existsCnf:
            self.server.hset(self.sbfMetaKey, self.REDIS_SBF_META_CONF_HASH_FIELD, pack(self.REDIS_CNF_FMT, self.scale, self.ratio,
                     self.initial_capacity, self.error_rate, self.filterErrorRate, self.max_filters))
            filterKeys = self.server.lrange(self.sbfFilterKeys, 0, -1)
            pipe = self.server.pipeline(transaction=True)
            for key in filterKeys:
                pipe.delete(key)
            # pipe.hset(self.sbfMetaKey, self.REDIS_SBF_META_COUNT_HASH_FIELD, 0)
            pipe.delete(self.sbfFilterKeys)
            pipe.execute()
        else:
            scale, ratio, initial_capacity, error_rate, filterErrorRate, max_filters = unpack(self.REDIS_CNF_FMT, existsCnf)
            if (self.scale, self.ratio, self.initial_capacity, self.error_rate, self.filterErrorRate, self.max_filters) != (scale, ratio, initial_capacity, error_rate, filterErrorRate, max_filters):
                raise ValueError("setup configure not match exists")
            

    def __contains__(self, key):
        """Tests a key's membership in this bloom filter.

        >>> b = ScalableRedisBloomFilter(server=server, bfkeypreffix="satest:bf", initial_capacity=512, error_rate=0.001, \
                                    max_filters = 5,
                                    mode=ScalableRedisBloomFilter.SMALL_SET_GROWTH)
        >>> b.add("hello")
        False
        >>> "hello" in b
        True

        """
        filterKeys = self._make_sure_filters()
        for filterKey in filterKeys: # reversed filters by create time 
            if key in self.filtersMap[filterKey]:
                return True
        return False

    def _split_filter_key(self, filterKey):
        filterKeyPrefix, capacity = filterKey.rsplit("||", 1)
        return filterKeyPrefix, capacity

    def _build_filter_key(self, filterKeyPrefix, capacity):
        return "%s||%s" % (filterKeyPrefix, capacity)

    def _make_sure_filters(self):
        filterKeys = self.server.lrange(self.sbfFilterKeys, 0, self.max_filters-1 if self.max_filters > 0 else -1)
        newFiltersMap = {}
        for filterKey in filterKeys:
            f = self.filtersMap.get(filterKey, None)
            if f is None:
                filterKeyPrefix, capacity = self._split_filter_key(filterKey)
                f = RedisBloomFilter(server=self.server, bfkeypreffix=filterKeyPrefix, capacity=int(capacity), error_rate=self.filterErrorRate)
            newFiltersMap[filterKey] = f
        self.filtersMap = newFiltersMap
        return filterKeys

    def create_filter(self):
        if self._try_get_lock():
            try:
                capacity = self.initial_capacity
                lastFilterKey = self.server.lrange(self.sbfFilterKeys, 0, 0)
                if lastFilterKey:
                    _, lastCapacity = self._split_filter_key(lastFilterKey[0])
                    capacity = int(lastCapacity) * self.scale
                    if capacity > self.MAX_FILTER_CAPACITY:
                        capacity = self.MAX_FILTER_CAPACITY
                filterIdx = self.server.hincrby(self.sbfMetaKey, self.REDIS_SBF_META_FILTER_IDX_HASH_FIELD, 1)
                filterKeyPrefix = self.REDIS_EACH_FILTER_KEY_FMT % (self.bfkeypreffix, filterIdx)
                filterKey = self._build_filter_key(filterKeyPrefix, capacity)
                self.server.lpush(self.sbfFilterKeys, filterKey)
                f = RedisBloomFilter(
                    server=self.server, 
                    bfkeypreffix = filterKeyPrefix,
                    capacity=capacity,
                    error_rate=self.filterErrorRate)
                self.filtersMap[filterKey] = f
                badFilterKeys = self.server.lrange(self.sbfFilterKeys, self.max_filters, -1)
                self.server.ltrim(self.sbfFilterKeys, 0, self.max_filters-1)
                if badFilterKeys:
                    self._delete_filters(badFilterKeys)
                return f
            except Exception,e:
                print e
                return None
            finally:
                self._release_lock()
        else:
            return None


    def _delete_filters(self, filterKeys):
        for filterKey in filterKeys:
            self._delete_filter(filterKey)

    def _delete_filter(self, filterKey):
        filterKeyPrefix, capacity = self._split_filter_key(filterKey)
        f = self.filtersMap.pop(filterKey, RedisBloomFilter(
                server=self.server, 
                bfkeypreffix = filterKeyPrefix,
                capacity=capacity,
                error_rate=self.filterErrorRate))
        f.clear()

    def _try_get_lock(self):
        return self.server.hsetnx(self.sbfMetaKey, self.REDIS_SBF_META_FILTERS_LOCK_HASH_FIELD, 1)

    def _release_lock(self):
        self.server.hdel(self.sbfMetaKey, self.REDIS_SBF_META_FILTERS_LOCK_HASH_FIELD)

    def add(self, key):
        """Adds a key to this bloom filter.
        If the key already exists in this filter it will return True.
        Otherwise False.

        >>> b = ScalableRedisBloomFilter(server=server, bfkeypreffix="satest:bf", initial_capacity=512, error_rate=0.001, \
                                    max_filters = 5,
                                    mode=ScalableRedisBloomFilter.SMALL_SET_GROWTH)
        >>> b.add("hello")
        False
        >>> b.add("hello")
        True

        """
        if key in self:
            return True
        filterKeys = self._make_sure_filters()
        lastFilter = None
        if filterKeys:
            lastFilter = self.filtersMap.get(filterKeys[0], None)
        if lastFilter is None or lastFilter.capacity <= lastFilter.count:
            lastFilter = self.create_filter()
            while lastFilter is None:
                filterKeys = self._make_sure_filters()
                if filterKeys:
                    lastFilter = self.filtersMap.get(filterKeys[0], None)
                if lastFilter is None or lastFilter.capacity <= lastFilter.count:
                    lastFilter = self.create_filter()
        lastFilter.add(key, skip_check=True)
        return False


    @property
    def capacity(self):
        """Returns the total capacity for all filters in this SBF"""
        self._make_sure_filters()
        return sum(f.capacity for f in self.filtersMap.values())

    @property
    def count(self):
        return len(self)

    def clear(self): 
        pipe = self.server.pipeline(transaction=True)
        pipe.delete(self.sbfMetaKey)
        pipe.lrange(self.sbfFilterKeys, 0, -1)
        pipe.delete(self.sbfFilterKeys)
        _,filterKeys,_,_ = pipe.execute()
        if filterKeys:
            self._delete_filters(filterKeys)
        

    def tofile(self, f):
        """Serialize this ScalableBloomFilter into the file-object
        `f'."""
        raise NotImplementedError("RedisBloomFilter not support tofile")

    @classmethod
    def fromfile(cls, f):
        """Deserialize the ScalableBloomFilter in file object `f'."""
        raise NotImplementedError("RedisBloomFilter not support fromfile")

    def __len__(self):
        """Returns the total number of elements stored in this SBF"""
        self._make_sure_filters()
        return sum(f.count for f in self.filtersMap.values())
