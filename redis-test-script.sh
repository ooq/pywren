#export redisname=pywren-noshard-test.oapxhs.0001.usw2.cache.amazonaws.com #cache.r3.large
export redisname=pywren-noshard-test2.oapxhs.0001.usw2.cache.amazonaws.com #cache.r3.2xlarge
#export redisname=pywren-noshard-test3.oapxhs.0001.usw2.cache.amazonaws.com #cache.t2.medium
#export redisname=pywren-sharding-test.oapxhs.clustercfg.usw2.cache.amazonaws.com

#cluster mode 
export redisname=pywren-sharding-test.oapxhs.clustercfg.usw2.cache.amazonaws.com

for i in 7
do
	#python clean-redis.py $redisname
	#records=$((1000000 / $i))
	records=100000
	#echo $records
	#python redis-clean-by-delete.py write --workers=1 --value_size 10000 --redis_hostname $redisname --num_per_lambda 100000
	python redis-append.py write --workers=$i --value_size 10000 --redis_hostname $redisname --num_per_lambda $records &
	python redis-read.py write --workers=$i --value_size 10000 --redis_hostname $redisname --num_per_lambda $records
done

python process_redis.py $redisname