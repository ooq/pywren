import pywren
import boto3
import md5
import numpy as np
import cPickle as pickle
import botocore

if __name__ == "__main__":
    import logging
    import subprocess
    import gc
    import time
    def run_command(key):
        pywren.wrenlogging.default_config()
        logger = logging.getLogger(__name__)


        client = boto3.client('s3', 'us-west-2')
        
        results = {}
        for i in range(0,1000):
            number_of_records = 1000 * 1000
            keyname = "input/part-" + str(key)
            m = md5.new()
            m.update(keyname)
            randomized_keyname = "input/" + m.hexdigest()[:8] + "-part-" + str(key)
            try:
                info = client.head_object(Bucket = "sort-data-random", Key = randomized_keyname)
                if info['ContentLength'] == 100000000:
                    results[key] = True
                else:
                    results[key] = False
            except botocore.exceptions.ClientError as e:
                results[key] = False
            key = key + 1
        return results

    wrenexec = pywren.default_executor(shard_runtime=True)
    #tasks = range(0,1000000,1000)
    tasks = range(0,1000000, 1000)
    #tasks = range(1)
    fut = wrenexec.map_sync_with_rate_and_retries(run_command, tasks, rate=100)

    pywren.wait(fut)
    res = [f.result() for f in fut]
    found = sum([a.values().count(True) for a in res])
    miss = sum([a.values().count(False) for a in res])
    print("Found : " + str(found) + " Miss : " + str(miss))
    #print res 
    pickle.dump(res, open('sort-input.pickle', 'wb'))
    #print res
