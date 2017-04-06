import time
import boto3 
import uuid
import numpy as np
import time
import pywren
import subprocess
import logging
import sys
import boto3
import hashlib
import cPickle as pickle
import uuid
import click
# this is in general a bad idea, but oh well. 
import sys
sys.path.append("../")
import exampleutils
import botocore
import md5
from rediscluster import StrictRedisCluster
import random

@click.group()
def cli():
    pass


def write(bucket_name, mb_per_file, number, key_prefix, 
          region):
    def clean_redis(key):
        redisnode = "pywren-redis-clu.oapxhs.clustercfg.usw2.cache.amazonaws.com"
        startup_nodes = [{"host": redisnode, "port": 6379}]
        r1 = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
        #r1 = redis.StrictRedis(host=redisnode, port=6379, db=0)
        #r1 = redis.StrictRedis(host=redisurl1, port=6379, db=0)
        #r1 = redis.StrictRedisCluster(startup_nodes=startup_nodes)
        #r1.set("foo", "bar")
        sizes = r1.dbsize()
        r1.flushall()
        return sizes

    def run_command(my_worker_id):
        redis_hostname = "pywren-redis-clu.oapxhs.clustercfg.usw2.cache.amazonaws.com"
        redis_port = 6379
        value_size = 5000
        num_per_lambda = 140000
        key_prefix = "pywren_redis"
        startup_nodes = [{"host": redis_hostname, "port": redis_port}]

        '''
        redis_client = redis.StrictRedis(host=redis_hostname, port=redis_port,
            password=redis_password, db=0)
        '''
        d = exampleutils.RandomDataGenerator(value_size).read(value_size)

        def work():
            redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
            alli  = range(num_per_lambda)
            random.shuffle(alli)
            for i in alli:
                key_name = key_prefix + '_' + str(i)
                #m = md5.new()
                #m.update(key_name)
                #randomized_keyname = m.hexdigest()[:8] + "-part-" + str(key_name)
                # Add some entropy to the values
                d1 = d
                try:
                    redis_client.append(key_name, d1)
                except redis.exceptions.RedisError:
                    print("redis error")
                    # return 0, 0, 0, 0
                    #logger = logging.getLogger(__name__)
                    #logger.warn("exception in redis")

        t1 = time.time()
        work()
        t2 = time.time()

        write_tput = num_per_lambda/(t2-t1)

        return t1, t2, write_tput

    wrenexec = pywren.default_executor(shard_runtime=True)

    futures = wrenexec.map_sync_with_rate_and_retries(clean_redis, range(1), rate=10000)
    pywren.wait(futures)
    results = [f.result() for f in futures]
    print("clean: " + str(results))
    # create list of random keys
    futures = wrenexec.map_sync_with_rate_and_retries(run_command, range(number), rate=10000)
    
    pywren.wait(futures) 
    results = [f.result() for f in futures]
    run_statuses = [f.run_status for f in futures]
    invoke_statuses = [f.invoke_status for f in futures]
    #print("write "+ str(results))


    res = {'results' : results, 
           'run_statuses' : run_statuses, 
           'bucket_name' : bucket_name, 
           'keynames' : range(number), 
           'invoke_statuses' : invoke_statuses}
    return res


def read(bucket_name, number, 
         keylist_raw, read_times, region):
    
    blocksize = 1024*1024

    def run_command(key):
        t1 = time.time()
        time.sleep(10) 
        t2 = time.time()

        return t1, t2, 1

    wrenexec = pywren.default_executor(shard_runtime=True)
    if number == 0:
        keylist = keylist_raw
    else:
        keylist = [keylist_raw[i % len(keylist_raw)]  for i in range(number)]
    
    futures = wrenexec.map_sync_with_rate_and_retries(run_command, keylist, rate=10000)

    pywren.wait(futures)
    results = [f.result() for f in futures]
    run_statuses = [f.run_status for f in futures]
    invoke_statuses = [f.invoke_status for f in futures]
    print("read "+ str(results))
    res = {'results' : results, 
           'run_statuses' : run_statuses, 
           'invoke_statuses' : invoke_statuses}
    return res


@cli.command('write')
@click.option('--bucket_name', help='bucket to save files in')
@click.option('--mb_per_file', help='MB of each object in S3', type=int)
@click.option('--number', help='number of files', type=int)
@click.option('--key_prefix', default='', help='S3 key prefix')
@click.option('--outfile', default='s3_benchmark.write.output.pickle', 
              help='filename to save results in')
@click.option('--region', default='us-west-2', help="AWS Region")
def write_command(bucket_name, mb_per_file, number, key_prefix, region, outfile):
    res = write(bucket_name, mb_per_file, number, key_prefix, region)
    pickle.dump(res, open(outfile, 'wb'))

@cli.command('read')
@click.option('--key_file', default=None, help="filename generated by write command, which contains the keys to read")
@click.option('--number', help='number of objects to read, 0 for all', type=int, default=0)
@click.option('--outfile', default='s3_benchmark.read.output.pickle', 
              help='filename to save results in')
@click.option('--read_times', default=1, help="number of times to read each s3 key")
@click.option('--region', default='us-west-2', help="AWS Region")
def read_command(key_file, number, outfile, 
                 read_times, region):

    d = pickle.load(open(key_file, 'rb'))
    bucket_name = d['bucket_name']
    keynames = d['keynames']
    res = read(bucket_name, number, keynames, 
               read_times, region)
    pickle.dump(res, open(outfile, 'wb'))

if __name__ == '__main__':
    cli()
