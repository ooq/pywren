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

@click.group()
def cli():
    pass


def write(bucket_name, mb_per_file, number, key_prefix, 
          region):

    def run_command(key_name):
        t1 = time.time()
        time.sleep(200)        
        t2 = time.time()


        return t1, t2, 1

    wrenexec = pywren.default_executor(shard_runtime=True)

    # create list of random keys
    keynames = [ key_prefix + str(uuid.uuid4().get_hex().upper()) for _ in range(number)]
    futures = wrenexec.map_sync_with_rate_and_retries(run_command, keynames, rate=10000)
    
    pywren.wait(futures) 
    results = [f.result() for f in futures]
    run_statuses = [f.run_status for f in futures]
    invoke_statuses = [f.invoke_status for f in futures]
    print("write "+ str(results))



    res = {'results' : results, 
           'run_statuses' : run_statuses, 
           'bucket_name' : bucket_name, 
           'keynames' : keynames, 
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
