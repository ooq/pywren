for file in *.pickle.breakdown.*
do
	aws s3 cp $file s3://exp-results/s3scaling/socc/
done
