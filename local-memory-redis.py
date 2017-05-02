import pywren
import time
from pywren import wrenlogging
import logging
import sys

import redis
from rediscluster import StrictRedisCluster


# redisurl1 = "pywren-redis.oapxhs.ng.0001.usw2.cache.amazonaws.com"
# redisurl1 = "pywren-redis-001.oapxhs.0001.usw2.cache.amazonaws.com"
# r1 = redis.StrictRedis(host=redisurl1, port=6379, db=0)

# redisurl1 = "pywren-sharding.oapxhs.clustercfg.usw2.cache.amazonaws.com"
# redisurl1 = "pywren-sharding-0001-001.oapxhs.0001.usw2.cache.amazonaws.com"
# redisurl_config = "pywren-sharding-test.oapxhs.clustercfg.usw2.cache.amazonaws.com"
# startup_nodes = [{"host": redisurl_config, "port": "6379"}]
# r1 = redis.StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)

redisnode = sys.argv[1]

def f(key):
	#wrenlogging.default_config()
	logger = logging.getLogger(__name__)
	logger.warn("Run")

	#redisurl1 = "pywren-sharding-test.oapxhs.clustercfg.usw2.cache.amazonaws.com"
	#redisurl1 = "pywren-noshard-test.oapxhs.0001.usw2.cache.amazonaws.com"
	#redisurl1 = "pywren-sharding-test-0001-001.oapxhs.0001.usw2.cache.amazonaws.com"
	#startup_nodes = [{"host": redisurl1, "port": "6379"}]
	#r1 = redis.StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
	#redisurl1 = "pywren-sharding-test-0001-001.oapxhs.0001.usw2.cache.amazonaws.com"
	#redisurl1 = "pywren-redis.oapxhs.ng.0001.usw2.cache.amazonaws.com"
	startup_nodes = [{"host": redisnode, "port": 6379}]
	r1 = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
	#r1 = redis.StrictRedis(host=redisnode, port=6379, db=0)
	#r1 = redis.StrictRedis(host=redisurl1, port=6379, db=0)
	#r1 = redis.StrictRedisCluster(startup_nodes=startup_nodes)
	#r1.set("foo", "bar")
	sizes = r1.dbsize()
        print(sizes)
        info = r1.info()
        print(info)
	return sizes
	#return 1
	#return 1
	#raise Exception("1", "2")
	#pywren.wrenlogging.default_config()
f(1)
