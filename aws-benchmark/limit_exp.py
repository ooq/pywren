import numpy as np
import time
import sys
sys.path.append("../")
import exampleutils
import pywren

import cPickle as pickle


import s3_benchmark_con as s3_benchmark

BUCKET_NAME = 's3scalingb'
AWS_REGION='us-west-2'


def run_exp(infile, outfile, workers, seed, mb_per_file):
    # write simply writes that number of keys
    print("running {} {} {} {}".format(outfile, workers, seed, mb_per_file))
    t1 = time.time()
    write_results = s3_benchmark.write(BUCKET_NAME, mb_per_file, workers, 
                                      key_prefix="", region=AWS_REGION)
    t2 = time.time()
    #read_results = s3_benchmark.read(BUCKET_NAME, workers, 
    #                                 write_results['keynames'], 
    #                                 read_times=1, 
    #                                 region=AWS_REGION)
    read_results = write_results
    t3 = time.time()
    pickle.dump({'write_results' : write_results, 
                 'read_results' : read_results, 
                 'write_start_timestamp' : t1,
                 'read_start_timestamp' : t1, 
                 'read_done_timestamp' : t3, 
                 'workers' : workers, 
                 'seed' : seed, 
                 'mb_per_file' : mb_per_file}, 
                open(outfile, 'w'), -1)

    print("workers=", workers, "seed=", seed, "mb_per_file=", mb_per_file, "runtime:{:3.0f}".format(t3-t1))
                 
if __name__ == "__main__":
    workers = int(sys.argv[1])
    seed = 1
    mb_per_file = 1000
    prefix = "rate_microbench.{}.{}.{}".format(workers, seed, mb_per_file)
    outfile = prefix + ".pickle"
    run_exp(None, outfile, workers, seed, mb_per_file)

