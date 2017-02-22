#!/usr/bin/env python
#
"""Test performance of BloomFilter at a set capacity and error rate."""
import sys
from pybloomredis import RedisBloomFilter, ScalableRedisBloomFilter
import math, time
from utils import range_fn
from redis import StrictRedis


def main(capacity=100000, request_error_rate=0.001):
    server = StrictRedis.from_url("redis://192.168.254.106:6379")
    bfkeypreffix="test-bf"
    keys = server.keys(bfkeypreffix + "*")
    for key in keys:
        server.delete(key)
    f = RedisBloomFilter(capacity=capacity, server=server, bfkeypreffix=bfkeypreffix, error_rate=request_error_rate)
    assert (capacity == f.capacity)
    start = time.time()
    for i in range_fn(0, f.capacity):
        f.add(i, skip_check=True)
    end = time.time()
    print("{:5.3f} seconds to add to capacity, {:10.2f} entries/second".format(
            end - start, f.capacity / (end - start)))
    keys = server.keys(bfkeypreffix + "*")
    oneBits = 0
    for key in keys:
        oneBits += server.bitcount(key)
    #print "Number of 1 bits:", oneBits
    print("Number of Filter Bits:", f.num_bits)
    print("Number of slices:", f.num_slices)
    print("Bits per slice:", f.bits_per_slice)
    print("------")
    print("Fraction of 1 bits at capacity: {:5.3f}".format(
            oneBits / float(f.num_bits)))
    # Look for false positives and measure the actual fp rate
    trials = f.capacity
    fp = 0
    start = time.time()
    for i in range_fn(f.capacity, f.capacity + trials + 1):
        if i in f:
            fp += 1
    end = time.time()
    print(("{:5.3f} seconds to check false positives, "
           "{:10.2f} checks/second".format(end - start, trials / (end - start))))
    print("Requested FP rate: {:2.4f}".format(request_error_rate))
    print("Experimental false positive rate: {:2.4f}".format(fp / float(trials)))
    # Compute theoretical fp max (Goel/Gupta)
    k = f.num_slices
    m = f.num_bits
    n = f.capacity
    fp_theory = math.pow((1 - math.exp(-k * (n + 0.5) / (m - 1))), k)
    print("Projected FP rate (Goel/Gupta): {:2.6f}".format(fp_theory))


def mainScalable(capacity=100000, request_error_rate=0.001):
    server = StrictRedis.from_url("redis://192.168.254.106:6379")
    bfkeypreffix="test-bf"
    keys = server.keys(bfkeypreffix + "*")
    for key in keys:
        server.delete(key)
    f = ScalableRedisBloomFilter(initial_capacity=10000, server=server, bfkeypreffix=bfkeypreffix, error_rate=request_error_rate, max_filters=5)
    # assert (capacity == f.capacity)
    start = time.time()
    for i in range_fn(0, capacity):
        f.add(i)
    end = time.time()
    print("{:5.3f} seconds to add to capacity, {:10.2f} entries/second".format(
            end - start, f.capacity / (end - start)))
    keys = server.keys(bfkeypreffix + "*")
    oneBits = 0
    for key in keys:
        oneBits += server.bitcount(key)
    #print "Number of 1 bits:", oneBits
    print("Number of Filter Bits:", sum(ff.num_bits for ff in f.filters))
    print("Number of slices:", sum(ff.num_slices for ff in f.filters))
    print("Bits per slice:", sum(ff.bits_per_slice for ff in f.filters))
    print("------")
    print("Fraction of 1 bits at capacity: {:5.3f}".format(
            oneBits / float(sum(ff.num_bits for ff in f.filters))))
    # Look for false positives and measure the actual fp rate
    trials = f.capacity
    fp = 0
    start = time.time()
    for i in range_fn(f.capacity, f.capacity + trials + 1):
        if i in f:
            fp += 1
    end = time.time()
    print(("{:5.3f} seconds to check false positives, "
           "{:10.2f} checks/second".format(end - start, trials / (end - start))))
    print("Requested FP rate: {:2.4f}".format(request_error_rate))
    print("Experimental false positive rate: {:2.4f}".format(fp / float(trials)))
    # Compute theoretical fp max (Goel/Gupta)
    # k = f.num_slices
    # m = f.num_bits
    # n = f.capacity
    # fp_theory = math.pow((1 - math.exp(-k * (n + 0.5) / (m - 1))), k)
    # print("Projected FP rate (Goel/Gupta): {:2.6f}".format(fp_theory))



if __name__ == '__main__' :
    main()
    mainScalable()
