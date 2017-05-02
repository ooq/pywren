for file in *bw*.pickle
do
	aws s3 cp $file s3://exp-results/s3scaling/bw/
done
