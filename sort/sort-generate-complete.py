import pywren
import boto3
import md5
import numpy as np
import cPickle as pickle

if __name__ == "__main__":
    import logging
    import subprocess
    import gc
    import time
    def run_command(keylist):
        pywren.wrenlogging.default_config()
        logger = logging.getLogger(__name__)


        client = boto3.client('s3', 'us-west-2')
        client.download_file('qifan-public', 'gensort', '/tmp/condaruntime/gensort')
        res = subprocess.check_output(["chmod",
                                        "a+x",
                                        "/tmp/condaruntime/gensort"])

        for key in keylist:
            number_of_records = 1000 * 1000
            begin = key * number_of_records
            data = subprocess.check_output(["/tmp/condaruntime/gensort",
                                            "-b"+str(begin),
                                            str(number_of_records),
                                            "/dev/stdout"])
            keyname = "input/part-" + str(key)
            m = md5.new()
            m.update(keyname)
            randomized_keyname = "input/" + m.hexdigest()[:8] + "-part-" + str(key)
            put_start = time.time()
            #client.put_object(Body = data, Bucket = "sort-data-random", Key = randomized_keyname)
            client.put_object(Body = data, Bucket = "sort-data-random", Key = randomized_keyname)
            put_end = time.time()
            logger.info(str(key) + " th object uploaded using " + str(put_end-put_start) + " seconds.")
            gc_start = time.time()
            gc.collect()
            gc_end = time.time()
        return keylist
    wrenexec = pywren.default_executor(shard_runtime=True)
    
    #fp = open("../finished_calls.txt","r")
    #finished_calls = []
    #data = fp.readline()
    #while data:
    #    finished_calls.append(int(data))
    #    data = fp.readline()
    #print(str(len(finished_calls)))
    #tasks = range(20,1000000,5)
    #unfinished_calls = list(set(range(len(tasks))).difference(set(finished_calls)))
    #unfinished_tasks = list(np.array(tasks)[unfinished_calls])
    #print(str(len(unfinished_tasks)))
    #passed_tasks = []
    #for iii in unfinished_tasks:
    #    passed_tasks.append(int(iii))
    #passed_tasks = range(0,1000000,5)
    passed_tasks = range(1)
    ut = pickle.load(open("sort-input.pickle", "rb"))
    
    utl = []
    for uti in ut:
        for b in uti:
            if not uti[b]:
                utl.append(b)
    tasks = []
    task = []
    for uti in utl:
        task.append(uti)
        if len(task) == 5:
            tasks.append(task)
            task = [] 
    tasks.append(task)
    #print tasks   
    fut = wrenexec.map_sync_with_rate_and_retries(run_command, tasks, rate=2000)

    pywren.wait(fut)
    res = [f.result() for f in fut]
    #print res
