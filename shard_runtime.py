from pywren import wrenutil
import boto3

s3_bucket = "qifan-public"
s3_key = "condaruntime.nomkl.glob2.shrinked.gensort.tar.gz"

client = boto3.client("s3")

for shard in range(50):
	key_shard = wrenutil.get_s3_shard(s3_key, shard)
	new_s3_key = wrenutil.hash_s3_key(key_shard)

	csource = s3_bucket + "/" + s3_key
	client.copy_object(Bucket=s3_bucket, CopySource=csource, Key=new_s3_key)
