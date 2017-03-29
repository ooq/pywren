export redisname=pywren-noshard-test.oapxhs.0001.usw2.cache.amazonaws.com
#export redisname=pywren-sharding-test.oapxhs.clustercfg.usw2.cache.amazonaws.com
for i in 50
do
	python clean-redis.py $redisname
	python redis-append.py write --workers=$i --value_size 10000 --redis_hostname $redisname --num_per_lambda 100000
done

python process_redis.py $redisname