for file in *.pickle
do
	aws s3 cp $file s3://exp-results/100sort/redis-benchmark/
done
