import pywren
import boto3
import md5
import numpy as np
import random
import os
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

        total = key['total']
        results = 0
        for i in range(1, total+1):
            table = key['table']
            scale = key['scale']
            index = i
            
            filename = table + "_" + str(index) + "_" + str(total) + ".csv"
            keyname = "scale" + str(scale) + "/" + table + "/" + filename
            try:
                info = client.head_object(Bucket = "qifan-tpcds-data", Key = keyname)
                results += 1
            except botocore.exceptions.ClientError as e:
                results += 0
            
        return table + ":" + str(results) + "/" + str(total)
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
    #passed_tasks = range(0,10000,5)
    #passed_tasks = range(0,1000000,5)
    #passed_tasks = range(1)
    tables_1000 = [("call_center",1),
    ("catalog_page",1),
    ("catalog_sales",3614),
    ("customer",18),
    ("customer_address",8),
    ("customer_demographics",1),
    ("date_dim",1),
    ("household_demographics",1),
    ("income_band",1),
    ("inventory",140),
    ("item",1),
    ("promotion",1),
    ("reason",1),
    ("ship_mode",1),
    ("store",1),
    ("store_sales",5248),
    ("time_dim",1),
    ("warehouse",1),
    ("web_page",1),
    ("web_sales",1808),
    ("web_site",1)]
    tables_10 = [("call_center",1),
    ("catalog_page",1),
    ("catalog_sales",33),
    ("customer",1),
    ("customer_address",1),
    ("customer_demographics",1),
    ("date_dim",1),
    ("household_demographics",1),
    ("income_band",1),
    ("inventory",27),
    ("item",1),
    ("promotion",1),
    ("reason",1),
    ("ship_mode",1),
    ("store",1),
    ("store_sales",44),
    ("time_dim",1),
    ("warehouse",1),
    ("web_page",1),
    ("web_sales",17),
    ("web_site",1)]
    all_tables = {10:tables_10, 1000: tables_1000}

    scale = 10
    passed_tasks = [] 
    for (table, total) in all_tables[scale]:
            key = {} 
            key['total'] = total
            key['scale'] = scale
            key['table'] = table
            passed_tasks.append(key)
        #if table is not "date_dim":
        #    continue
        #for key in passed_tasks:
            print(run_command(key))
        #continue
    #fut = wrenexec.map_sync_with_rate_and_retries(run_command, passed_tasks, rate=300)

    pywren.wait(fut)
    res = [f.result() for f in fut]
    print(res)    
    #exit(0)
