for i in `seq 1 10`
do
	python clean-redis.py tpcds${i}.oapxhs.0001.usw2.cache.amazonaws.com
done
