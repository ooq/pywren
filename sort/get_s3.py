import boto3
import numpy as np

client = boto3.client('s3', 'us-west-2')

obj = client.get_object(Bucket='sort-data', Key='input1/part1-1')
fileobj = obj['Body']

keyType = np.dtype([('key', 'S4')])
# 4 bytes good enough for partitioning
recordType = np.dtype([('key','S4'), ('value', 'S96')])

data = np.fromstring(fileobj.read(), dtype = recordType)
records = data

record_list = []
for record in records:
	record_list.append(record)

body = np.asarray(record_list).tobytes()

#client.put_object(Bucket='sort-data', Key='input1/part1-1', Body=records.tobytes())
client.put_object(Bucket='sort-data', Key='input1/part1-1', Body=body)




