from ruffus import * 
import numpy as np
import time
import sys
sys.path.append("../")
import exampleutils
import pywren

import exampleutils
import cPickle as pickle


import s3_benchmark_rate as s3_benchmark


def ruffus_params():
    #for workers in [1, 10, 30, 100, 300, 600, 1000, 2000, 2800]:
    #for workers in [1400, 1401, 1402, 1403, 1404, 1405]:
    for workers in [1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000, 6500, 7000, 7500, 8000, 8500, 9000, 9500, 10000]:
        for seed in range(1):
            for mb_per_file in [1000]: # 10, 100, 1000]:
        
                prefix = "s3.bw4.{}.{}.{}".format(workers, seed, 
                                                             mb_per_file)
                outfile = prefix + ".pickle"
                yield None, outfile, workers, seed, mb_per_file

BUCKET_NAME = 's3scalingb'
AWS_REGION='us-west-2'

@files(ruffus_params)

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
    pipeline_run([run_exp])

