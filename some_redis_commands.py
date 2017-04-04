import redis
ra = redis.StrictRedis(host="10.0.48.134")
rb = redis.StrictRedis(host="10.0.80.189")
rc = redis.StrictRedis(host="10.0.32.2")

from rediscluster import StrictRedisCluster
redisurl1 = "pywren-sharding-test.oapxhs.clustercfg.usw2.cache.amazonaws.com"
startup_nodes = [{"host": redisurl1, "port": "6379"}]
cl = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)


from rediscluster import StrictRedisCluster
redisurl1 = "10.0.48.134"
startup_nodes = [{"host": redisurl1, "port": 6379}]
cl = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
cl.dbsize()