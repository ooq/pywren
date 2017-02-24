import sys
import struct
import time
import numpy as np
from itertools import groupby
import boto3
import pywren
import pywren.wrenlogging
import cPickle as pickle
from multiprocessing.pool import ThreadPool
#from multiprocessing import Pool
import logging
import base64
from botocore.exceptions import ClientError
import random

def partition_data():
        def run_command(key):
                pywren.wrenlogging.default_config('WARN')
                begin_of_function = time.time()
                logger = logging.getLogger(__name__)
                logger.warn("taskId = " + str(key['taskId']))
                logger.warn("number of inputs = " + str(key['inputs']))
                logger.warn("number of output partitions = " + str(key['parts']))

                # TODO: make the parameters configurable
                taskId = key['taskId']
                # 1T
                totalInputs = 10000
                inputsPerTask = key['inputs']
                taskPerRound =  1
                rounds = (inputsPerTask + taskPerRound - 1) / taskPerRound
                numPartitions = key['parts']

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

                client = boto3.client('s3', 'us-west-2')
                table = boto3.resource('dynamodb', 'us-west-2').Table('sort-4')

                [t1, t2, t3] = [time.time()] * 3
                # a total of 10 threads
                read_pool = ThreadPool(1)
                write_pool = ThreadPool(7)
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

                        logger.warn("Range for round " + str(roundIdx) + " is (" + str(startId) + "," + str(endId) + ")")
                        
                        # before processing, make sure all data is read
                        read_pool.map(read_work, inputIds)

                        records = np.concatenate(inputs)

                        t1 = time.time()
                        logger.warn('read time ' + str(t1-t3))

                        if numPartitions == 1:
                                ps = [0] * len(records)
                        else:
                                ps = np.searchsorted(boundaries, records['key'])
                        t2 = time.time()
                        logger.warn('calculating partitions time: ' + str(t2-t1))

                        # before processing the newly read data, make sure outputs are all written out
                        if len(write_pool_handler_container) > 0:
                                write_pool_handler = write_pool_handler_container.pop()
                                twait_start = time.time()
                                write_pool_handler.wait()
                                twait_end = time.time()
                                if twait_end - twait_start > 0.5:
                                        logger.warn('write time = ' + str(twait_end-t3) + " slower than read " + str(t1-t3))
                                else:
                                        logger.warn('write time < ' + str(twait_end-t3) + " faster than read " + str(t1-t3))

                        t2 = time.time()
                        outputs = [[] for i in range(0, numPartitions)]
                        for idx,record in enumerate(records):
                                        #if idx % 100000 == 0:
                                        #        logger.warn('paritioning record idx: ' + str(idx))
                                        outputs[ps[idx]].append(record)
                        t3 = time.time()
                        logger.warn('paritioning time: ' + str(t3-t2))

                        def write_work(roundIdx):
                                #key = "intermediate/part-" + str(mapId) + "-" + str(partitionId)
                                # client.put_object(Bucket='sort-data-2', Key=key, Body=body)
                                mapId = rounds * taskId + roundIdx
                                #logger.warn("trying to open batch writer")
                                with table.batch_writer() as batch:
                                    #logger.warn("batch writer opened")
                                    tstart = time.time()
                                    try:
                                        for i in range(numPartitions):
                                            filekey = mapId * numPartitions + i
                                            if (filekey % 50 == 0):
                                                logger.warn("filekey: " + str(filekey))
                                            # dynamo requires base64 encoding for bytes
                                            body = base64.b64encode(np.asarray(outputs[ps[i]]).tobytes())
                                            response = batch.put_item(
                                            #table.put_item(
                                                        Item={
                                                            'filkey': filekey,
                                                            'file': {'B': body},
                                                        }
                                            )
                                    except ClientError as e:
                                        logger.error("Received error: %s", e, exc_info=True)
                                    tend = time.time()
                                    #logger.warn("finished in " + str(tend-tstart) + " seconds.")
                        def write_work_test(writer_key):
                                #key = "intermediate/part-" + str(mapId) + "-" + str(partitionId)
                                # client.put_object(Bucket='sort-data-2', Key=key, Body=body)
                                mapId = rounds * taskId + writer_key['roundIdx']
                                try:
                                    #filekey = mapId * numPartitions + writer_key['i']
                                    # if (filekey % 50 == 0):
                                    #     logger.warn("filekey: " + str(filekey))
                                    # dynamo requires base64 encoding for bytes
                                    filekey_s = "m-" + str(mapId) + "-" + str(writer_key['i']) + "-good"
                                    body = base64.b64encode(np.asarray(outputs[ps[i]]).tobytes())
                                    #response = batch.put_item(
                                    table.put_item(
                                                Item={
                                                    #'filkey': random.randint(0, 4294967295),
                                                    'filekey': filekey_s,
                                                    'file': {'B': body},
                                                }
                                    )
                                    #if (filekey % 50 == 0):
                                        #logger.warn("response: " + str(response))
                                except ClientError as e:
                                    logger.error("Received error: %s", e, exc_info=True)
                        def write_work_test_2(writer_key):
                                #key = "intermediate/part-" + str(mapId) + "-" + str(partitionId)
                                # client.put_object(Bucket='sort-data-2', Key=key, Body=body)
                                mapId = rounds * taskId + writer_key['roundIdx']
                                logger.warn("trying to open batch writer")
                                with table.batch_writer() as batch:
                                    logger.warn("batch writer opened")
                                    tstart = time.time()
                                    try:
                                        for i in range(25):
                                            #filekey = mapId * numPartitions + min(i + 100 * writer_idx, numPartitions-1)
                                            partId = i + 25 * writer_key['writer_idx']
                                            if partId >= numPartitions:
                                                break;
                                            #if (filekey % 50 == 0):
                                            #    logger.warn("filekey: " + str(filekey))
                                            filekey_s = "m-" + str(mapId) + "-" + str(partId) + "-good"
                                            # dynamo requires base64 encoding for bytes
                                            body = base64.b64encode(np.asarray(outputs[ps[i]]).tobytes())
                                            response = batch.put_item(
                                            #table.put_item(
                                                        Item={
                                                            'filekey': filekey_s,
                                                            'file': {'B': body},
                                                        }
                                            )
                                    except ClientError as e:
                                        logger.error("Received error: %s", e)
                                    tend = time.time()
                                    logger.warn("finished in " + str(tend-tstart) + " seconds.")

                        paritions = range(numPartitions)
                        writer_keylist = []
                        for i in range(numPartitions):
                                writer_keylist.append({'roundIdx': roundIdx,
                                                'i': i})

                        #write_pool_handler = write_pool.map_async(write_work, [roundIdx])
                        write_pool_handler = write_pool.map_async(write_work_test, writer_keylist)
                        #write_pool_handler = write_pool.map_async(write_work_test_2, writer_keylist)
                        write_pool_handler_container.append(write_pool_handler)

                if len(write_pool_handler_container) > 0:
                        write_pool_handler = write_pool_handler_container.pop()
                        twait_start = time.time()
                        write_pool_handler.wait()
                        twait_end = time.time()
                        logger.warn('last write time = ' + str(twait_end-t3))
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

        pywren.wait(fut)
        res = [f.result() for f in fut]
        print res
        pickle.dump(res, open('sort.db.part.output.pickle', 'w'))


if __name__ == '__main__':
        partition_data()

