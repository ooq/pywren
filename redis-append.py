"""
Benchmark aggregate read/write performance to S3
$ python redis_benchmark.py write --value_size=1024 --num_per_lambda=80
Saves the output in a pickled list of throughputs.
"""

import time
import boto3 
import botocore
import uuid
import redis

import time
import pywren
import subprocess
import logging
import sys
import hashlib
import cPickle as pickle
import click
import exampleutils

import md5

from collections import deque

@click.group()
def cli():
    pass


@cli.command()
@click.option('--workers', help='number of lambda workers', type=int)
@click.option('--redis_hostname', help='hostname running redis')
@click.option('--redis_port', help='redis port to connect to', type=int, default=6379)
@click.option('--redis_password', help='redis password to use')
@click.option('--value_size', help='size of values to insert in bytes', type=int, default=1024)
@click.option('--num_per_lambda', help='number of keys to insert from each lambda', type=int, default=10000)
@click.option('--key_prefix', default='redis_', 
              help='prefix to use for redis keys')
@click.option('--outfile', default='redis_benchmark.write.output.pickle', 
              help='filename to save results in')
def write(workers, redis_hostname, redis_port, redis_password, 
        value_size, num_per_lambda, key_prefix, outfile):

    print "redis_hostname =", redis_hostname
    print "redis_port =", redis_port
    print "value_size=", value_size
    print "num_per_lambda=", num_per_lambda

    def run_command(my_worker_id):
        redis_client = redis.StrictRedis(host=redis_hostname, port=redis_port,
            password=redis_password, db=0)
        d = exampleutils.RandomDataGenerator(value_size).read(value_size)

        t1 = time.time()
        for i in xrange(num_per_lambda):
            key_name = key_prefix + '_' + str(i)
            m = md5.new()
            m.update(key_name)
            randomized_keyname = m.hexdigest()[:8] + "-part-" + str(key_name)
            # Add some entropy to the values
            d1 = d + str(my_worker_id) + str(i)
            redis_client.append(randomized_keyname, d1)
        t2 = time.time()

        write_tput = num_per_lambda/(t2-t1)

        return t1, t2, (t2-t1), write_tput

    wrenexec = pywren.default_executor()

    fut = wrenexec.map(run_command, range(workers))

    res = [f.result() for f in fut]
    print res
    pickle.dump(res, open(redis_hostname + ".032901.pickle."  + str(workers), 'w'))

if __name__ == '__main__':
    cli()
