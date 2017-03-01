import sys
import struct
import time
import numpy as np
from itertools import groupby
import boto3
import pywren
import cPickle as pickle
from multiprocessing.pool import ThreadPool
from multiprocessing import Manager
#from multiprocessing import Pool
import logging
import md5
import gc

def run_command(key):
        client = boto3.client('s3', 'us-west-2')
        pywren.wrenlogging.default_config('INFO')
        logger = logging.getLogger(__name__)
        bucketName = "sort-data-random-1t"
        def read_work(i):
                keyname = "input/part-" + str(i)
                m = md5.new()
                m.update(keyname)
                randomized_keyname = "input/" + m.hexdigest()[:8] + "-part-" + str(i)
                logger.info("fetching " + randomized_keyname)
                obj = client.get_object(Bucket=bucketName, Key=randomized_keyname)
        read_work(1)
        return 1

def remote():
        wrenexec = pywren.default_executor()
        fut = wrenexec.map(run_command, range(1))

        pywren.wait(fut)
        res = [f.result() for f in fut]
        print res

def local():
        res = run_command(1)
        print res

if __name__ == '__main__':
        remote()
        #local()

