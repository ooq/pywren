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
import botocore

def partition_data():
        def run_command(key):
                pywren.wrenlogging.default_config('INFO')
                begin_of_function = time.time()
                logger = logging.getLogger(__name__)
                logger.info("taskId = " + str(key['taskId']))
                logger.info("number of works = " + str(key['works']))
                logger.info("number of input partitions = " + str(key['parts']))

                bucketName = key['bucket']
                taskId = key['taskId']
                rounds = key['works']
                numPartitions = key['parts']

                # 10 bytes for sorting
                recordType = np.dtype([('key','S10'), ('value', 'S90')])

                client = boto3.client('s3', 'us-west-2')

                [t1, t2, t3] = [time.time()] * 3
                # a total of 10 threads
                write_pool = ThreadPool(1)
                number_of_clients = 10
                read_pool = ThreadPool(number_of_clients)
                clients = []
                for client_id in range(number_of_clients):
                        clients.append(boto3.client('s3', 'us-west-2'))
                read_pool_handler_container = []
                for roundIdx in range(rounds):
                        inputs = []

                        def read_work(reader_key):
                                client_id = reader_key['client_id']
                                local_client = clients[client_id]
                                reduceId = rounds * taskId + reader_key['roundIdx']
                                key_per_client = reader_key['key-per-client']

                                for mapId in range(key_per_client*client_id, min(key_per_client*(client_id+1), numPartitions)):
                                        keyname = "shuffle/part-" + str(mapId) + "-" + str(reduceId)
                                        m = md5.new()
                                        m.update(keyname)
                                        randomized_keyname = "shuffle/" + m.hexdigest()[:8] + "-part-" + str(mapId) + "-" + str(reduceId)
                                        try:
                                            obj = local_client.get_object(Bucket=bucketName, Key=randomized_keyname)
                                        except botocore.exceptions.ClientError as e:
                                            logger.info("reading error key " + randomized_keyname)
                                            raise
                                        else:
                                            fileobj = obj['Body']
                                            data = np.fromstring(fileobj.read(), dtype = recordType)
                                            inputs.append(data)

                        reader_keylist = []
                        key_per_client = (numPartitions + number_of_clients - 1) / number_of_clients
                        for client_id in range(number_of_clients):
                                reader_keylist.append({'roundIdx': roundIdx,
                                                'client_id': client_id,
                                                'key-per-client':key_per_client})

                        read_pool.map(read_work, reader_keylist)
                        t1 = time.time()
                        logger.info('read time ' + str(t1-t3))

                        if len(write_pool_handler_container) > 0:
                                write_pool_handler = write_pool_handler_container.pop()
                                twait_start = time.time()
                                write_pool_handler.wait()
                                twait_end = time.time()
                                if twait_end - twait_start > 0.5:
                                        logger.info('write time = ' + str(twait_end-t3) + " slower than read " + str(t1-t3))
                                else:
                                        logger.info('write time < ' + str(twait_end-t3) + " faster than read " + str(t1-t3))


                        records = np.concatenate(inputs)
                        gc.collect()

                        t2 = time.time()
                        records.sort(order='key')
                        t3 = time.time()
                        logger.info('sort time: ' + str(t3-t2))

                        def write_work(reduceId):
                                keyname = "output/part-" + str(reduceId)
                                m = md5.new()
                                m.update(keyname)
                                randomized_keyname = "output/" + m.hexdigest()[:8] + "-part-" + str(reduceId)
                                body = records.tobytes()
                                client.put_object(Bucket=bucketName, Key=randomized_keyname, Body=body)

                        write_pool_handler = write_pool.map_async(write_work, [taskId * rounds + roundIdx])
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
        worksPerTask = int(sys.argv[2])
        numPartitions = int(sys.argv[3])

        keylist = []

        for i in range(numTasks):
                keylist.append({'taskId': i,
                                'works': worksPerTask,
                                'parts': numPartitions,
                                'bucket': "sort-data-random-1t"})

        wrenexec = pywren.default_executor()
        fut = wrenexec.map(run_command, keylist)

        pywren.wait(fut)
        res = [f.result() for f in fut]
        print res
        pickle.dump(res, open('sort.s3.sort.output.pickle', 'w'))


if __name__ == '__main__':
        partition_data()

