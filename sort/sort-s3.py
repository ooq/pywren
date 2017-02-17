import sys
import struct
import binascii
import time
import numpy as np

chunksize = 100
taskId = int(sys.argv[1])
numPartitions = int(sys.argv[2])

recordType = np.dtype([('key','S10'), ('value', 'S90')])

t0 = time.time()
inputs = []
for i in range(0, numPartitions):
        filename = "part-" + str(i) + "-" + str(taskId)
        with open(filename, "rb") as fp:
                inputs.append(np.fromfile(fp, dtype=recordType))
        fp.close()

records = np.concatenate(inputs)
t1 = time.time()
print 'read time', (t1-t0)

t0 = time.time()
records.sort(order='key')
t1 = time.time()
print 'sort time', (t1-t0)


t0 = time.time()
outfilename = "part-" + str(taskId) + ".sort"
with open(outfilename, "wb") as fo:
        records.tofile(fo)
fo.close()
t1 = time.time()
print 'write time', (t1-t0)