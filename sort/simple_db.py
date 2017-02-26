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
                # begin_of_function = time.time()
                # logger = logging.getLogger(__name__)
                table = boto3.resource('dynamodb', 'us-west-2').Table('sort-2')
                # #client = boto3.client('s3', 'us-west-2')

                # #obj = client.get_object(Bucket='sort-data', Key="input/part-0")
                # r = table.put_item(
                #     Item={
                #         'filekey': 4000+key['taskId'],
                #     }
                # )
                # #logger.info(response['Item']) 

                # end_of_function = time.time()
                # response = table.put_item(
                #     Item={
                #         'filekey': key['taskId'],
                #     }
                # )

                def write_work():
                        with table.batch_writer() as batch:
                            for i in range(10):
                                filekey = i
                                body = np.asarray(range(1)).tobytes()
                                response = batch.put_item(
                                            Item={
                                                'filekey': filekey,
                                                'file': body
                                            }
                                )
                                #logger.info('key ' + str(filekey))
                                #logger.info('response ' + str(response))
                write_work()

                # return r
                return key

        numTasks = int(sys.argv[1])

        keylist = []
        for i in range(numTasks):
                keylist.append({'taskId': i})

        wrenexec = pywren.default_executor()
        fut = wrenexec.map(run_command, keylist)


        t0 = time.time()
        pywren.wait(fut)
        t1 = time.time()
        print('wait is ', (t1-t0))
        res = [f.result() for f in fut]
        t2 = time.time()
        print('resilt is ', (t2-t1))
        print res
        # print res
        # pickle.dump(res, open('db.part.output.pickle', 'w'))


if __name__ == '__main__':
        partition_data()

