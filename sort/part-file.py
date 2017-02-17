import sys
import struct
import time
import numpy as np
from itertools import groupby

chunksize = 100
taskId = int(sys.argv[1])
numPartitions = int(sys.argv[2])

min_value = struct.unpack(">I", "\x00\x00\x00\x00")[0]
max_value = struct.unpack(">I", "\xff\xff\xff\xff")[0]

rangePerPart = (max_value - min_value) / numPartitions

keyType = np.dtype([('key', 'S4')])

boundaries = []
# (numPartitions-1) boundaries
for i in range(1, numPartitions):
        # 4 bytes unsigned integers
        b = struct.pack('>I', rangePerPart * i)
        boundaries.append(b)

# 4 bytes good enough for partitioning
recordType = np.dtype([('key','S4'), ('value', 'S96')])

t0 = time.time()
filename = "part-" + str(taskId)
with open(filename, "rb") as fp:
        records = np.fromfile(fp, dtype=recordType)
fp.close()
t1 = time.time()
print 'read time ', (t1-t0)

t0 = time.time()
if numPartitions == 1:
        ps = [0] * len(records)
else:
        ps = np.searchsorted(boundaries, records['key'])
t1 = time.time()
print 'calculating partitions time: ', (t1-t0)

t0 = time.time()
outputs = [[] for i in range(0, numPartitions)]
for idx,record in enumerate(records):
                outputs[ps[idx]].append(record)
t1 = time.time()
print 'paritioning time: ', (t1-t0)

t0 = time.time()
for i in range(0, numPartitions):
        ofilename = "part-" + str(i) + "-" + str(taskId)
        with open(ofilename, "wb") as fo:
                np.asarray(outputs[ps[i]], dtype=recordType).tofile(fo)
        fo.close()
t1 = time.time()
print 'write time ', (t1-t0)
