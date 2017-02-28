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
import md5
import gc

def partition_data():
        def run_command(key):
                pywren.wrenlogging.default_config('INFO')
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
                bucketName = key['bucket']

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

                [t1, t2, t3] = [time.time()] * 3
                # a total of 10 threads
                read_pool = ThreadPool(1)
                number_of_clients = 10
                write_pool = ThreadPool(number_of_clients)
                clients = []
                for client_id in range(number_of_clients):
                        clients.append(boto3.client('s3', 'us-west-2'))
                write_pool_handler_container = []
                for roundIdx in range(rounds):
                        inputs = []

                        def read_work(inputId):
                                keyname = "input/part-" + str(inputId)
                                m = md5.new()
                                m.update(keyname)
                                randomized_keyname = "input/" + m.hexdigest()[:8] + "-part-" + str(inputId)
                                obj = client.get_object(Bucket=bucketName, Key=randomized_keyname)
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
                        gc.collect()

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
                        gc.collect()
                        outputs = [[] for i in range(0, numPartitions)]
                        for idx,record in enumerate(records):
                                        #if idx % 100000 == 0:
                                        #        logger.info('paritioning record idx: ' + str(idx))
                                        outputs[ps[idx]].append(record)
                        t3 = time.time()
                        logger.info('paritioning time: ' + str(t3-t2))

                        # def write_work(writer_key):
                        #         mapId = rounds * taskId + writer_key['roundIdx']
                        #         key = "part-" + str(mapId) + "-" + str(writer_key['i'])
                        #         m = md5.new()
                        #         m.update(key)
                        #         randomized_keyname = m.hexdigest()[:8] + key
                        #         body = np.asarray(outputs[ps[i]]).tobytes()
                        #         client.put_object(Bucket='sort-data-random-test', Key=randomized_keyname, Body=body)
                        def write_work_client(writer_key):
                                client_id = writer_key['i']
                                local_client = clients[client_id]
                                mapId = rounds * taskId + writer_key['roundIdx']
                                key_per_client = writer_key['key-per-client']

                                for i in range(key_per_client*client_id, min(key_per_client*(client_id+1), numPartitions)):
                                        keyname = "shuffle/part-" + str(mapId) + "-" + str(i)
                                        m = md5.new()
                                        m.update(keyname)
                                        randomized_keyname = "shuffle/" + m.hexdigest()[:8] + "-part-" + str(mapId) + "-" + str(i)
                                        body = np.asarray(outputs[ps[i]]).tobytes()
                                        local_client.put_object(Bucket=bucketName, Key=randomized_keyname, Body=body)

                        # writer_keylist = []
                        # for i in range(numPartitions):
                        #         writer_keylist.append({'roundIdx': roundIdx,
                        #                         'i': i})

                        writer_keylist = []
                        key_per_client = (numPartitions + number_of_clients - 1) / number_of_clients
                        for i in range(number_of_clients):
                                writer_keylist.append({'roundIdx': roundIdx,
                                                'i': i,
                                                'key-per-client':key_per_client})

                        write_pool_handler = write_pool.map_async(write_work_client, writer_keylist)
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
                                'parts': numPartitions,
                                'bucket': "sort-data-random-1t"})

        wrenexec = pywren.default_executor()
        fut = wrenexec.map(run_command, keylist)

        pywren.wait(fut)
        res = [f.result() for f in fut]
        print res
        pickle.dump(res, open('sort.s3.part.output.pickle', 'w'))


if __name__ == '__main__':
        partition_data()

