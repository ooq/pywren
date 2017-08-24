import sys
import struct
import time
import numpy as np
from itertools import groupby
from collections import defaultdict 
import boto3
import pywren
import cPickle as pickle
from multiprocessing.pool import ThreadPool
from multiprocessing import Manager
#from multiprocessing import Pool
import logging
import md5
import gc
from rediscluster import StrictRedisCluster

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
                inputsPerMerge = key['inputs']
                rounds =  key['merges']
                inputsPerTask = inputsPerMerge * rounds
                bucketName = key['bucket']

                min_value = struct.unpack(">I", "\x00\x00\x00\x00")[0]
                max_value = struct.unpack(">I", "\xff\xff\xff\xff")[0]

                rangePerPart = (max_value - min_value) / numPartitions

                keyType = np.dtype([('key', 'S4')])
                # 4 bytes good enough for partitioning
                recordType = np.dtype([('key','S4'), ('value', 'S96')])


                client = boto3.client('s3', 'us-west-2')

                [t1, t2, t3] = [time.time()] * 3
                [read_time, work_time, write_time] = [0]*3
                # a total of 10 threads
                read_pool = ThreadPool(1)
                number_of_clients = 1
                write_pool = ThreadPool(number_of_clients)
                clients = []
                for client_id in range(number_of_clients):
                        clients.append(boto3.client('s3', 'us-west-2'))
                write_pool_handler_container = []
                #manager = Manager()

                records_per_input = 1000000
                for roundIdx in range(rounds):
                        inputs = []

                        startId = taskId*inputsPerTask + roundIdx*inputsPerMerge
                        endId = min(taskId*inputsPerTask + min((roundIdx+1)*inputsPerMerge, inputsPerTask), totalInputs)
                        inputIds = range(startId, endId)
                        if (len(inputIds) == 0):
                                break

                        logger.info("Range for round " + str(roundIdx) + " is (" + str(startId) + "," + str(endId) + ")")

                        read_keylist = []
                        for i in range(len(inputIds)):
                            read_keylist.append({'inputId': inputIds[i],
                                                   'i': i})

                        def read_work(read_key):
                                inputId = read_key['inputId']
                                keyname = "input/part-" + str(inputId)
                                m = md5.new()
                                m.update(keyname)
                                randomized_keyname = "input/" + m.hexdigest()[:8] + "-part-" + str(inputId)
                                logger.info("fetching " + randomized_keyname)
                                obj = client.get_object(Bucket=bucketName, Key=randomized_keyname)
                                logger.info("fetching " + randomized_keyname + " done")
                                fileobj = obj['Body']
                                fileobjread = fileobj.read()
                                data = np.frombuffer(fileobjread, dtype = recordType)
                                logger.info("conversion " + randomized_keyname + " done")
                                logger.info("size " + randomized_keyname + "  " + str(len(data)))
                                i = read_key['i']
                                #records[i*records_per_input : (i+1)*records_per_input] = data
                                inputs.append(data)

                        # before processing, make sure all data is read
                        #read_pool.map(read_work, read_keylist)
                        for read_key in read_keylist:
                            read_work(read_key)
                        logger.info("read call done ")
                        logger.info("size of inputs" + str(len(inputs)))

                        print(records.size)
                        #inputs = []
                        gc.collect()

                        t1 = time.time()
                        t2 = time.time()
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
                        logger.info("partitioning start")
                        t3 = time.time()
                        logger.info('paritioning time: ' + str(t3-t2))
                        work_time = t3-t1
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

                                keyname = "shuffle/part-" + str(mapId) + "-" + str(0)
                                m = md5.new()
                                m.update(keyname)
                                randomized_keyname = "shuffle/" + m.hexdigest()[:8] + "-part-" + str(mapId) + "-" + str(0)
                                
                                mpu = local_client.create_multipart_upload(Bucket=bucketName, Key=randomized_keyname)

                                part_info = {
                                    'Parts': []
                                }

                                for i in range(len(inputIds)):
                                    part_number = i+1
                                    #part_data = records[i*records_per_input : (i+1)*records_per_input]
                                    part_data = inputs[0]
                                    part_data_ser = np.asarray(part_data).tobytes()
                                    logger.info('start upload part : ' + str(part_number))

                                    part = local_client.upload_part(Bucket=bucketName, Key=randomized_keyname, 
                                                            PartNumber=part_number,
                                                           UploadId=mpu['UploadId'], Body=part_data_ser)
                                    logger.info('finish upload part : ' + str(part_number))
                                    part_info['Parts'].append({'PartNumber': part_number, 'ETag': part['ETag']})
                                    del part_data
                                    del part_data_ser
                                    del inputs[0]
                                    
                                    #body = np.asarray(records).tobytes()
                                    #body = np.asarray(outputs[ps[i]]).tobytes()
                                    #local_client.put_object(Bucket=bucketName, Key=randomized_keyname, Body=body)
                                    #ridx = int(m.hexdigest()[:8], 16) % nrs 
                                    #rs[ridx].set(randomized_keyname, body)
                                    
                                local_client.complete_multipart_upload(Bucket=bucketName,
                                                                        Key=randomized_keyname, 
                                                                        UploadId=mpu['UploadId'],
                                                                        MultipartUpload=part_info)

                        # writer_keylist = []
                        # for i in range(numPartitions):
                        #         writer_keylist.append({'roundIdx': roundIdx,
                        #                         'i': i})

                        writer_keylist = []
                        for i in range(number_of_clients):
                                writer_keylist.append({'roundIdx': roundIdx,
                                                'i': i})

                        #write_pool_handler = write_pool.map_async(write_work_client, writer_keylist)
                        #write_pool_handler_container.append(write_pool_handler)
                        for write_key in writer_keylist:
                            write_work_client(write_key)

                if len(write_pool_handler_container) > 0:
                        write_pool_handler = write_pool_handler_container.pop()
                        twait_start = time.time()
                        write_pool_handler.wait()
                        twait_end = time.time()
                        logger.info('last write time = ' + str(twait_end-t3))
                        write_time = twait_end-t3
                read_pool.close()
                write_pool.close()
                read_pool.join()
                write_pool.join()

                end_of_function = time.time()
                return begin_of_function, end_of_function, read_time, work_time, write_time

        numTasks = int(sys.argv[1])
        inputsPerMerge = int(sys.argv[2])
        mergesPerTask = int(sys.argv[3])
        rate=int(sys.argv[4])

        keylist = []

        for i in range(numTasks):
                keylist.append({'taskId': i,
                                'inputs': inputsPerMerge,
                                'merges': mergesPerTask,
                                'bucket': "sort-data-random"})
        # a = ['00123']
        # for i in range(len(a)):
        #         keylist.append({'taskId': int(a[i]),
        #         'inputs': inputsPerTask,
        #         'parts': numPartitions,
        #         'bucket': "sort-data-random-1t"})

        wrenexec = pywren.default_executor()
        futures = wrenexec.map_sync_with_rate_and_retries(run_command, keylist, rate=rate)

        pywren.wait(futures)
        results = [f.result() for f in futures]
        print results
        run_statuses = [f.run_status for f in futures]
        invoke_statuses = [f.invoke_status for f in futures]
        res = {'results' : results,
              'run_statuses' : run_statuses,
              'invoke_statuses' : invoke_statuses}
        filename = "pywren-mem-merge." + str(rate) + ".pickle.breakdown." + str(len(redisnode.split(";")))
        pickle.dump(res, open(filename, 'w'))
        return res
        #run_command(keylist[0])


if __name__ == '__main__':
        partition_data()

