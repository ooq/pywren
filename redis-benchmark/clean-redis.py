import pywren
import time
from pywren import wrenlogging
import logging
import sys

import redis
from rediscluster import StrictRedisCluster


def f(key):
	redisnode = "pywren-redis-clu.oapxhs.clustercfg.usw2.cache.amazonaws.com"
	startup_nodes = [{"host": redisnode, "port": 6379}]
	r1 = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
	#r1 = redis.StrictRedis(host=redisnode, port=6379, db=0)
	#r1 = redis.StrictRedis(host=redisurl1, port=6379, db=0)
	#r1 = redis.StrictRedisCluster(startup_nodes=startup_nodes)
	#r1.set("foo", "bar")
	sizes = r1.dbsize()
	r1.flushall()
	return sizes
	#return 1
	#return 1
	#raise Exception("1", "2")
	#pywren.wrenlogging.default_config()

wrenexec = pywren.default_executor(job_max_runtime=200)
#wrenexec = pywren.dummy_executor()
futures = wrenexec.map(f, range(1))

pywren.wait(futures)
res = [f.result() for f in futures]
print res
