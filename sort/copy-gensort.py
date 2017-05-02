import pywren
import boto3
import md5
import numpy as np

client = boto3.client('s3', 'us-west-2')
for i in range(0,50):
    m = md5.new()
    m.update(str(i))
    randomized_keyname =  m.hexdigest()[:8] + "-gensort-" + str(i)
    bucket = "qifan-public"
    csource = bucket + "/" + "gensort"
    client.copy_object(CopySource=csource, Bucket =bucket, Key = randomized_keyname)
