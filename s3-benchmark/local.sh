export redisname=pywren-noshard-test2.oapxhs.0001.usw2.cache.amazonaws.com #cache.r3.2xlarge

#cluster mode 
export redisname=pywren-sharding-test.oapxhs.clustercfg.usw2.cache.amazonaws.com

for i in 1
do
    #records=$((1000000 / $i))
    records=100000
    #echo $records
    #python redis-clean-by-delete.py write --workers=1 --value_size 10000 --redis_hostname $redisname --num_per_lambda 100000
    python redis-append-local.py write --run_id 4 --workers=$i --value_size 10000 --redis_hostname $redisname --num_per_lambda $records &
    python redis-append-local.py write --run_id 5 --workers=$i --value_size 10000 --redis_hostname $redisname --num_per_lambda $records &
    python redis-append-local.py write --run_id 6 --workers=$i --value_size 10000 --redis_hostname $redisname --num_per_lambda $records &
    python redis-append-local.py write --run_id 7 --workers=$i --value_size 10000 --redis_hostname $redisname --num_per_lambda $records &
    python redis-append-local.py write --run_id 8 --workers=$i --value_size 10000 --redis_hostname $redisname --num_per_lambda $records &
    python redis-append-local.py write --run_id 9 --workers=$i --value_size 10000 --redis_hostname $redisname --num_per_lambda $records &
    python redis-append-local.py write --run_id 0 --workers=$i --value_size 10000 --redis_hostname $redisname --num_per_lambda $records &
    python redis-append-local.py write --run_id 2 --workers=$i --value_size 10000 --redis_hostname $redisname --num_per_lambda $records &
    python redis-append-local.py write --run_id 3 --workers=$i --value_size 10000 --redis_hostname $redisname --num_per_lambda $records &
    python redis-append-local.py write --run_id 1 --workers=$i --value_size 10000 --redis_hostname $redisname --num_per_lambda $records
done

#python process_redis.py $redisname
