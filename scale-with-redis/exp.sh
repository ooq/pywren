for node in 5 10
do
for i in `seq 1 5`
do
	bash run_clean_redis.sh
	python scale-with-redis.py $node $i
done
done
