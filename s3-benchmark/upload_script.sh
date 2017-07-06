for file in *bw4*.pickle
do
	aws s3 cp $file s3://exp-results/s3scaling/bw4/
done
