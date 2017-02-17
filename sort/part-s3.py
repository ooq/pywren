import sys
import struct
import time
import numpy as np
from itertools import groupby
import boto3

client = boto3.client('s3', 'us-west-2')

taskId = int(sys.argv[1])
inputsPerTask = int(sys.argv[2])
numPartitions = int(sys.argv[3])

min_value = struct.unpack(">I", "\x00\x00\x00\x00")[0]
max_value = struct.unpack(">I", "\xff\xff\xff\xff")[0]

rangePerPart = (max_value - min_value) / numPartitions

keyType = np.dtype([('key', 'S4')])
# 4 bytes good enough for partitioning
recordType = np.dtype([('key','S4'), ('value', 'S96')])

boundaries = []
# (numPartitions-1) boundaries
for i in range(1, numPartitions):
        # 4 bytes unsigned integers
        b = struct.pack('>I', rangePerPart * i)
        boundaries.append(b)


t0 = time.time()
client = boto3.client('s3', 'us-west-2')
inputs = []


for inputId in range((taskId*inputsPerTask), ((taskId+1)*inputsPerTask)):
        key = "input/part-" + str(inputId)
        obj = client.get_object(Bucket='sort-data', Key=key)
        fileobj = obj['Body']
        data = np.fromstring(fileobj.read(), dtype = recordType)
        inputs.append(data)
records = np.concatenate(inputs)


t1 = time.time()
print 'read time ', (t1-t0)

if numPartitions == 1:
        ps = [0] * len(records)
else:
        ps = np.searchsorted(boundaries, records['key'])
t2 = time.time()
print 'calculating partitions time: ', (t2-t1)

outputs = [[] for i in range(0, numPartitions)]
for idx,record in enumerate(records):
                outputs[ps[idx]].append(record)
t3 = time.time()
print 'paritioning time: ', (t3-t2)

for i in range(0, numPartitions):
        key = "intermediate/part-" + str(taskId) + "-" + str(i)
        body = np.asarray(outputs[ps[i]]).tobytes()
        client.put_object(Bucket='sort-data', Key=key, Body=body)
t4 = time.time()
print 'write time ', (t4-t3)


