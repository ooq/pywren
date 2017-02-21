import sys
import struct
import time
import numpy as np
from itertools import groupby
import boto3
import pywren
import cPickle as pickle
from multiprocessing.pool import ThreadPool
#from multiprocessing import Pool
import logging

def partition_data():
        def run_command(key):
                begin_of_function = time.time()
                logger = logging.getLogger(__name__)
                logger.info("taskId = " + str(key['taskId']))
                logger.info("number of inputs = " + str(key['inputs']))
                logger.info("number of output partitions = " + str(key['parts']))

                # TODO: make the parameters configurable
                taskId = key['taskId']
                # 1T
                totalInputs = 10000
                inputsPerTask = key['inputs']
                taskPerRound =  3
                rounds = (inputsPerTask + taskPerRound - 1) / taskPerRound
                numPartitions = key['parts']

                recordType = np.dtype([('key','S10'), ('value', 'S90')])

                client = boto3.client('s3', 'us-west-2')
                table = boto3.resource('dynamodb', 'us-west-2').Table('sort-intermediate')

                [t1, t2, t3] = [time.time()] * 3
                # a total of 10 threads
                read_pool = ThreadPool(1)
                write_pool = ThreadPool(1)
                write_pool_handler_container = []
                for roundIdx in range(rounds):
                        inputs = []

                        def read_work(inputId):
                                key = "input/part-" + str(inputId)
                                obj = client.get_object(Bucket='sort-data', Key=key)
                                fileobj = obj['Body']
                                data = np.fromstring(fileobj.read(), dtype = recordType)
                                inputs.append(data)

                        startId = taskId*inputsPerTask + roundIdx*taskPerRound
                        endId = min(taskId*inputsPerTask + min((roundIdx+1)*taskPerRound, inputsPerTask), totalInputs)
                        inputIds = range(startId, endId)
                        if (len(inputIds) == 0):
                                break

                        logger.info("Range for round " + str(roundIdx) + " is (" + str(startId) + "," + str(endId) + ")")
                        
                        # before processing, make sure all data is read
                        read_pool.map(read_work, inputIds)

                        records = np.concatenate(inputs)

                        t1 = time.time()
                        logger.info('read time ' + str(t1-t3))

                        if numPartitions == 1:
                                ps = [0] * len(records)
                        else:
                                ps = np.searchsorted(boundaries, records['key'])
                        t2 = time.time()
                        logger.info('calculating partitions time: ' + str(t2-t1))

                        # before processing the newly read data, make sure outputs are all written out
                        if len(write_pool_handler_container) > 0:
                                write_pool_handler = write_pool_handler_container.pop()
                                twait_start = time.time()
                                write_pool_handler.wait()
                                twait_end = time.time()
                                if twait_end - twait_start > 0.5:
                                        logger.info('write time = ' + str(twait_end-t3) + " slower than read " + str(t1-t3))
                                else:
                                        logger.info('write time < ' + str(twait_end-t3) + " faster than read " + str(t1-t3))

                        t2 = time.time()
                        outputs = [[] for i in range(0, numPartitions)]
                        for idx,record in enumerate(records):
                                        #if idx % 100000 == 0:
                                        #        logger.info('paritioning record idx: ' + str(idx))
                                        outputs[ps[idx]].append(record)
                        t3 = time.time()
                        logger.info('paritioning time: ' + str(t3-t2))

                        def write_work(partitionId):
                                #key = "intermediate/part-" + str(mapId) + "-" + str(partitionId)
                                # client.put_object(Bucket='sort-data-2', Key=key, Body=body)
                                with table.batch_writer() as batch:
                                    for i in range(numPartitions):
                                        filekey = mapId * numPartitions + i
                                        body = np.asarray(outputs[ps[i]]).tobytes()
                                        batch.put_item(
                                                    Item={
                                                        'filekey': filekey,
                                                        'file': body
                                                    }
                                        )
                        paritions = range(numPartitions)
                        write_pool_handler = write_pool.map_async(write_work, [1])
                        write_pool_handler_container.append(write_pool_handler)

                if len(write_pool_handler_container) > 0:
                        write_pool_handler = write_pool_handler_container.pop()
                        twait_start = time.time()
                        write_pool_handler.wait()
                        twait_end = time.time()
                        logger.info('last write time = ' + str(twait_end-t3))
                read_pool.close()
                write_pool.close()
                read_pool.join()
                write_pool.join()

                end_of_function = time.time()
                return begin_of_function, end_of_function

        numTasks = int(sys.argv[1])
        inputsPerTask = int(sys.argv[2])
        numPartitions = int(sys.argv[3])

        keylist = []
        for i in range(numTasks):
                keylist.append({'taskId': i,
                                'inputs': inputsPerTask,
                                'parts': numPartitions})

        wrenexec = pywren.default_executor()
        fut = wrenexec.map(run_command, keylist)

        res = [f.result() for f in fut]
        print res
        pickle.dump(res, open('sort.part.output.pickle', 'w'))


if __name__ == '__main__':
        partition_data()

