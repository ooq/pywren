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
                table = boto3.resource('dynamodb', 'us-west-2').Table('sort-1')
                client = boto3.client('s3', 'us-west-2')

                obj = client.get_object(Bucket='sort-data', Key="input/part-0")
                # response = table.get_item(
                #     Key={
                #         'filekey': 1,
                #     }
                # )

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

