
# coding: utf-8

# In[1]:




# In[16]:

import cPickle as pickle
import pandas as pd
import os
import numpy as np
import datetime
import table_schemas
from table_schemas import * 
import sys
sys.path.append('/Users/qifan/anaconda/envs/test-environment/lib/python2.7/site-packages/s3fs-0.1.2-py2.7.egg/')
from s3fs import S3FileSystem
import pywren

import redis
from rediscluster import StrictRedisCluster

import time
from hashlib import md5

from io import StringIO
import boto3
from io import BytesIO
from multiprocessing.pool import ThreadPool

import logging
import random


# SELECT
#   count(DISTINCT ws_order_number) AS `order count `,
#   sum(ws_ext_ship_cost) AS `total shipping cost `,
#   sum(ws_net_profit) AS `total net profit `
# FROM
#   web_sales ws1, date_dim, customer_address, web_site
# WHERE
#   d_date BETWEEN '1999-02-01' AND
#   (CAST('1999-02-01' AS DATE) + INTERVAL 60 days)
#     AND ws1.ws_ship_date_sk = d_date_sk
#     AND ws1.ws_ship_addr_sk = ca_address_sk
#     AND ca_state = 'IL'
#     AND ws1.ws_web_site_sk = web_site_sk
#     AND web_company_name = 'pri'
#     AND EXISTS(SELECT *
#                FROM web_sales ws2
#                WHERE ws1.ws_order_number = ws2.ws_order_number
#                  AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
#     AND NOT EXISTS(SELECT *
#                    FROM web_returns wr1
#                    WHERE ws1.ws_order_number = wr1.wr_order_number)
# ORDER BY count(DISTINCT ws_order_number)
# LIMIT 100


# In[2]:


#mode = 'local'

scale = 1000
parall_1 = 1000
parall_2 = 1000
parall_3 = 1000
#mode = 'local'
#mode = 's3-only'
mode = 's3-redis'
pywren_rate = 2000


n_buckets = 1



    

redis_hostname = "tpcds-large2.oapxhs.0001.usw2.cache.amazonaws.com"
redisnode = "tpcds-large.oapxhs.clustercfg.usw2.cache.amazonaws.com"
hostnames = ["tpcds1.oapxhs.0001.usw2.cache.amazonaws.com"]
'''
hostnames = ["tpcds1.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds2.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds3.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds4.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds5.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds6.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds7.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds8.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds9.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds10.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds11.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds12.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds13.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds14.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds15.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds16.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds17.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds18.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds19.oapxhs.0001.usw2.cache.amazonaws.com",
             "tpcds20.oapxhs.0001.usw2.cache.amazonaws.com"]
'''
n_nodes = len(hostnames)
startup_nodes = [{"host": redisnode, "port": 6379}]
instance_type = "cache.r3.8xlarge"


wrenexec = pywren.default_executor(shard_runtime=True)

#stage_info_load = pickle.load(open("stageinfo.pickle", "r"))
stage_info_load = {}

pm = [str(parall_1), str(parall_2), str(parall_3), str(pywren_rate), str(n_nodes)]
filename = "cluster-" +  mode + '-tpcds-q94-scale' + str(scale) + "-" + "-".join(pm) + "-b" + str(n_buckets) + ".pickle"
#filename = "simple-test.pickle"

print("Scale is " + str(scale))


if mode == 'local':
    temp_address = "/Users/qifan/data/q1-temp/"
else:
    temp_address = "scale" + str(scale) + "/q1-temp/"


# In[3]:


def get_type(typename):
    if typename == "date":
        return datetime.datetime
    if "decimal" in typename:
        return np.dtype("float")
    if typename == "int" or typename == "long":
        return np.dtype("float")
    if typename == "float":
        return np.dtype(typename)
    if typename == "string":
        return np.dtype(typename)
    raise Exception("Not supported type: " + typename)


def get_s3_locations(table):
    print("WARNING: get from S3 locations, might be slow locally.")
    s3 = S3FileSystem()
    ls_path = os.path.join("qifan-tpcds-data", "scale" + str(scale), table)
    all_files = s3.ls(ls_path)
    return ["s3://" + f for f in all_files if f.endswith(".csv")]

def get_local_locations(table):
    print("WARNING: get from local locations, might not work on lamdbda.")
    files = []
    path = "/Users/qifan/data/tpcds-scale10/" + table
    for f in os.listdir(path):
        if f.endswith(".csv"):
            files.append(os.path.join(path, f))
    return files

def get_name_for_table(tablename):
    schema = table_schemas.schemas[tablename]
    names = [a[0] for a in schema]
    return names

def get_dtypes_for_table(tablename):
    schema = table_schemas.schemas[tablename]
    dtypes = {}
    for a,b in schema:
        dtypes[a] = get_type(b)
    return dtypes
    


# In[17]:


def read_local_table(key):
    loc = key['loc']
    names = list(key['names'])
    names.append("")
    dtypes = key['dtypes']
    parse_dates = []
    for d in dtypes:
        if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
            parse_dates.append(d)
            dtypes[d] = np.dtype("string")
    part_data = pd.read_table(loc, 
                              delimiter="|", 
                              header=None, 
                              names=names,
                              usecols=range(len(names)-1), 
                              dtype=dtypes, 
                              na_values = "-",
                              parse_dates=parse_dates)
    #print(part_data.info())
    return part_data

def read_s3_table(key, s3_client=None):
    loc = key['loc']
    names = list(key['names'])
    names.append("")
    dtypes = key['dtypes']
    parse_dates = []
    for d in dtypes:
        if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
            parse_dates.append(d)
            dtypes[d] = np.dtype("string")
    if s3_client == None:
        s3_client = boto3.client("s3")
    data = []
    if isinstance(key['loc'], str):
        loc = key['loc']
        obj = s3_client.get_object(Bucket='qifan-tpcds-data', Key=loc[22:])['Body'].read()
        data.append(obj)
    else:
        for loc in key['loc']:
            obj = s3_client.get_object(Bucket='qifan-tpcds-data', Key=loc[22:])['Body'].read()
            data.append(obj)
    part_data = pd.read_table(BytesIO("".join(data)),
                              delimiter="|", 
                              header=None, 
                              names=names,
                              usecols=range(len(names)-1), 
                              dtype=dtypes, 
                              na_values = "-",
                              parse_dates=parse_dates)
    #print(part_data.info())
    return part_data



# In[18]:


def hash_key_to_index(key, number):
    return int(md5(key).hexdigest()[8:], 16) % number

def my_hash_function(row, indices):
    # print indices
    #return int(sha1("".join([str(row[index]) for index in indices])).hexdigest()[8:], 16) % 65536
    #return hashxx("".join([str(row[index]) for index in indices]))% 65536
    #return random.randint(0,65536)
    return hash("".join([str(row[index]) for index in indices])) % 65536

def add_bin(df, indices, bintype, partitions):
    #tstart = time.time()
    
    # loopy way to compute hvalues
    #values = []
    #for _, row in df.iterrows():
    #    values.append(my_hash_function(row, indices))
    #hvalues = pd.DataFrame(values)
    
    # use apply()
    hvalues = df.apply(lambda x: my_hash_function(tuple(x), indices), axis = 1)
    
    #print("here is " + str(time.time() - tstart))
    #print(hvalues)
    #print("here is " + str(time.time() - tstart))
    if bintype == 'uniform':
        #_, bins = pd.qcut(samples, partitions, retbins=True, labels=False)
        bins = np.linspace(0, 65536, num=(partitions+1), endpoint=True)
    elif bintype == 'sample':
        samples = hvalues.sample(n=min(hvalues.size, max(hvalues.size/8, 65536)))
        _, bins = pd.qcut(samples, partitions, retbins=True, labels=False)
    else:
        raise Exception()
    #print("here is " + str(time.time() - tstart))
    df['bin'] = pd.cut(hvalues, bins=bins, labels=False, include_lowest=False)
    #print("here is " + str(time.time() - tstart))
    return bins


def write_local_intermediate(table, output_loc):
    output_info = {}
    table.to_csv(output_loc, sep="|", header=False, index=False)
    output_info['loc'] = output_loc
    output_info['names'] = table.columns
    output_info['dtypes'] = table.dtypes
    return output_info

def write_s3_intermediate(output_loc, table, s3_client=None):
    csv_buffer = BytesIO()
    table.to_csv(csv_buffer, sep="|", header=False, index=False)
    if s3_client == None:
        s3_client = boto3.client('s3')
        
    bucket_index = int(md5(output_loc).hexdigest()[8:], 16) % n_buckets
    s3_client.put_object(Bucket="qifan-tpcds-" + str(bucket_index),
                         Key=output_loc,
                         Body=csv_buffer.getvalue())
    output_info = {}
    output_info['loc'] = output_loc
    output_info['names'] = table.columns
    output_info['dtypes'] = table.dtypes

    return output_info

def write_redis_intermediate(output_loc, table, redis_client=None):
    csv_buffer = BytesIO()
    slt_columns = table.columns.delete(table.columns.get_loc('bin'))
    table.to_csv(csv_buffer, sep="|", header=False, index=False, columns=slt_columns)
    if redis_client == None:
        #redis_client = redis.StrictRedis(host=redis_hostname, port=6379, db=0)
        redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
    redis_client.set(output_loc, csv_buffer.getvalue())

    output_info = {}
    output_info['loc'] = output_loc
    output_info['names'] = table.columns
    output_info['dtypes'] = table.dtypes

    return output_info

def write_local_partitions(df, column_names, bintype, partitions, storage):
    #print(df.columns)
    t0 = time.time()
    indices = [df.columns.get_loc(myterm) for myterm in column_names]
    #print(indices)
    bins = add_bin(df, indices, bintype, partitions)
    #print((bins))
    #print(df)
    t1 = time.time()
    outputs_info = []
    for bin_index in range(len(bins)):
        split = df[df['bin'] == bin_index]
        if split.size > 0:
            output_info = {}
            split.drop('bin', axis=1, inplace=True)
            #print(split.dtypes)
            # write output to storage
            output_loc = storage + str(bin_index) + ".csv"
            outputs_info.append(write_local_intermediate(split, output_loc))
            #print(split.size)
    t2 = time.time()
    
    results = {}
    results['outputs_info'] = outputs_info
    results['breakdown'] = [(t1-t0), (t2-t1)]
    return results

def write_s3_partitions(df, column_names, bintype, partitions, storage):
    #print(df.columns)
    t0 = time.time()
    indices = [df.columns.get_loc(myterm) for myterm in column_names]
    #print(indices)
    bins = add_bin(df, indices, bintype, partitions)
    t1 = time.time()
    # print("t1 - t0 is " + str(t1-t0))
    #print((bins))
    #print(df)
    s3_client = boto3.client("s3")
    outputs_info = []
    def write_task(bin_index):
        split = df[df['bin'] == bin_index]
        if split.size > 0 or split.size < 1:
            # print(split.size)
            split.drop('bin', axis=1, inplace=True)
            #print(split.dtypes)
            # write output to storage
            output_loc = storage + str(bin_index) + ".csv"
            outputs_info.append(write_s3_intermediate(output_loc, split, s3_client))
    write_pool = ThreadPool(1)
    write_pool.map(write_task, range(len(bins)))
    write_pool.close()
    write_pool.join()
    t2 = time.time()
    
    results = {}
    results['outputs_info'] = outputs_info
    results['breakdown'] = [(t1-t0), (t2-t1)]
    return results


def write_redis_partitions(df, column_names, bintype, partitions, storage):
    #print(df.columns)
    t0 = time.time()
    indices = [df.columns.get_loc(myterm) for myterm in column_names]
    #print(indices)
    bins = add_bin(df, indices, bintype, partitions)
    t1 = time.time()
    # print("t1 - t0 is " + str(t1-t0))
    #print((bins))
    #print(df)
    redis_clients = []
    pipes = []
    for hostname in hostnames:
        #redis_client = redis.StrictRedis(host=hostname, port=6379, db=0)
        redis_client = redis.Redis(host=hostname, port=6379, db=0)
        redis_clients.append(redis_client)
        pipes.append(redis_client.pipeline())
    #redis_client = redis.StrictRedis(host=redis_hostname, port=6379, db=0)
    
    #redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
            
    outputs_info = []
    def write_task(bin_index):
        split = df[df['bin'] == bin_index]
        if split.size > 0 or split.size < 1:
            # print(split.size)
            #split.drop('bin', axis=1, inplace=True)
            #print(split.dtypes)
            # write output to storage
            output_loc = storage + str(bin_index) + ".csv"
            redis_index = hash_key_to_index(output_loc, len(hostnames))
            #redis_client = redis_clients[redis_index]
            redis_client = pipes[redis_index]
            outputs_info.append(write_redis_intermediate(output_loc, split, redis_client))
    write_pool = ThreadPool(1)
    write_pool.map(write_task, range(len(bins)))
    write_pool.close()
    write_pool.join()
    #for i in range(len(bins)):
    #    write_task(i)
    t2 = time.time()

    for pipe in pipes:
        pipe.execute()
    for redis_client in redis_clients:
        redis_client.connection_pool.disconnect()
    
    results = {}
    results['outputs_info'] = outputs_info
    results['breakdown'] = [(t1-t0), (t2-t1)]
    return results


def read_local_intermediate(key):
    names = list(key['names'])
    dtypes = key['dtypes']
    parse_dates = []
    for d in dtypes:
        if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
            parse_dates.append(d)
            dtypes[d] = np.dtype("string")
    part_data = pd.read_table(key['loc'], 
                              delimiter="|", 
                              header=None, 
                              names=names,
                              dtype=dtypes, 
                              parse_dates=parse_dates)
    return part_data

def read_s3_intermediate(key, s3_client=None):
    bucket_index = int(md5(key['loc']).hexdigest()[8:], 16) % n_buckets
    
    names = list(key['names'])
    dtypes = key['dtypes']
    parse_dates = []
    for d in dtypes:
        if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
            parse_dates.append(d)
            dtypes[d] = np.dtype("string")
    if s3_client == None:
        s3_client = boto3.client("s3")
    #print('qifan-tpcds-' + str(bucket_index))
    #print(key['loc'])
    obj = s3_client.get_object(Bucket='qifan-tpcds-' + str(bucket_index), Key=key['loc'])
    #print(key['loc'] + "")
    part_data = pd.read_table(BytesIO(obj['Body'].read()), 
                              delimiter="|", 
                              header=None, 
                              names=names,
                              dtype=dtypes, 
                              parse_dates=parse_dates)
    #print(part_data.info())
    return part_data

def read_redis_intermediate(key, redis_client=None):
    #bucket_index = int(md5(key['loc']).hexdigest()[8:], 16) % n_buckets
    
    names = list(key['names'])
    dtypes = key['dtypes']
    parse_dates = []
    for d in dtypes:
        if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
            parse_dates.append(d)
            dtypes[d] = np.dtype("string")
    if redis_client == None:
        #redis_client = redis.StrictRedis(host=redis_hostname, port=6379, db=0)
        redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
  
    part_data = pd.read_table(BytesIO(redis_client.get(key['loc'])), 
                              delimiter="|", 
                              header=None, 
                              names=names,
                              dtype=dtypes, 
                              parse_dates=parse_dates)
    #print(part_data.info())
    return part_data

def convert_buffer_to_table(names, dtypes, data):
    #bucket_index = int(md5(key['loc']).hexdigest()[8:], 16) % n_buckets
    
    parse_dates = []
    for d in dtypes:
        if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
            parse_dates.append(d)
            dtypes[d] = np.dtype("string")
  
    part_data = pd.read_table(BytesIO(data),
                              delimiter="|", 
                              header=None, 
                              names=names,
                              dtype=dtypes, 
                              parse_dates=parse_dates)
    #print(part_data.info())
    return part_data


def mkdir_if_not_exist(path):
    if mode == 'local':
        get_ipython().system(u'mkdir -p $path ')

def read_local_multiple_splits(names, dtypes, prefix, number_splits, suffix):
    key = {}
    key['names'] = names
    dtypes_dict = {}
    for i in range(len(names)):
        dtypes_dict[names[i]] = dtypes[i]
    key['dtypes'] = dtypes_dict
    ds = []
    for i in range(number_splits):
        key['loc'] = prefix + str(i) + suffix
        d = read_local_intermediate(key)
        ds.append(d)
    return pd.concat(ds)

def read_s3_multiple_splits(names, dtypes, prefix, number_splits, suffix):
    dtypes_dict = {}
    for i in range(len(names)):
        dtypes_dict[names[i]] = dtypes[i]
        
    ds = []
    s3_client = boto3.client("s3")

    def read_work(split_index):
        key = {}
        key['names'] = names
        key['dtypes'] = dtypes_dict
        key['loc'] = prefix + str(split_index) + suffix
        d = read_s3_intermediate(key, s3_client)
        ds.append(d)
    
    read_pool = ThreadPool(1)
    read_pool.map(read_work, range(number_splits))
    read_pool.close()
    read_pool.join()

    return pd.concat(ds)

def read_redis_multiple_splits(names, dtypes, prefix, number_splits, suffix):
    dtypes_dict = {}
    for i in range(len(names)):
        dtypes_dict[names[i]] = dtypes[i]
        
    ds = []
    #redis_client = redis.StrictRedis(host=redis_hostname, port=6379, db=0)
    #redis_client = StrictRedisCluster(startup_nodes=startup_nodes, skip_full_coverage_check=True)
    redis_clients = []
    pipes = []
    for hostname in hostnames:
        #redis_client = redis.StrictRedis(host=hostname, port=6379, db=0)
        redis_client = redis.Redis(host=hostname, port=6379, db=0)
        redis_clients.append(redis_client)
        pipes.append(redis_client.pipeline())

    def read_work(split_index):
        key = {}
        #key['names'] = names
        #key['dtypes'] = dtypes_dict
        key['loc'] = prefix + str(split_index) + suffix
        redis_index = hash_key_to_index(key['loc'], len(hostnames))
        #redis_client = redis_clients[redis_index]
        pipes[redis_index].get(key['loc'])
        #redis_client = pipes[redis_index]
        #d = read_redis_intermediate(key, redis_client)
        #ds.append(d)
   
    #read_pool = ThreadPool(64)
    #read_pool.map(read_work, range(number_splits))
    #read_pool.close()
    #read_pool.join()
    for i in range(number_splits):
        read_work(i)
    ps = time.time()
    read_data = []
    for pipe in pipes:
        current_data = pipe.execute()
        #print(len(current_data))
        read_data.extend(current_data)
    pe = time.time()
    #print("pipe time : " + str(pe-ps))
    for redis_client in redis_clients:
        redis_client.connection_pool.disconnect()

    #return pd.concat(ds)
    return convert_buffer_to_table(names, dtypes_dict, "".join(read_data))

def read_table(key):
    if mode == "local":
        return read_local_table(key)
    else:
        return read_s3_table(key)

def read_multiple_splits(names, dtypes, prefix, number_splits, suffix):
    if mode == "local":
        return read_local_multiple_splits(names, dtypes, prefix, number_splits, suffix)
    elif mode == "s3-only":
        return read_s3_multiple_splits(names, dtypes, prefix, number_splits, suffix)
    else:
        return read_redis_multiple_splits(names, dtypes, prefix, number_splits, suffix)

def read_intermediate(key):
    if mode == "local":
        return read_local_intermediate(key)
    elif mode == "s3-only":
        return read_s3_intermediate(key)
    else:
        return read_redis_intermediate(key)

def write_intermediate(table, output_loc):
    if mode == "local":
        return write_local_intermediate(table, output_loc)
    elif mode == "s3-only":
        return write_s3_intermediate(table, output_loc)
    else:
        return write_reids_intermediate(table, output_loc)

    
def write_partitions(df, column_names, bintype, partitions, storage):
    if mode == "local":
        return write_local_partitions(df, column_names, bintype, partitions, storage)
    elif mode == "s3-only":
        return write_s3_partitions(df, column_names, bintype, partitions, storage)
    else:
        return write_redis_partitions(df, column_names, bintype, partitions, storage)

    
def get_locations(table):
    if mode == "local":
        return get_local_locations(table)
    else:
        return get_s3_locations(table)


# In[19]:




# In[4]:



def execute_s3_stage(stage_function, tasks):
    t0 = time.time()
    #futures = wrenexec.map(stage_function, tasks)
    #pywren.wait(futures, 1, 64, 1)
    
    futures = wrenexec.map_sync_with_rate_and_retries(stage_function, tasks, straggler=True, WAIT_DUR_SEC=5, rate=pywren_rate)
    
    results = [f.result() for f in futures]
    run_statuses = [f.run_status for f in futures]
    invoke_statuses = [f.invoke_status for f in futures]
    t1 = time.time()
    res = {'results' : results,
           't0' : t0,
           't1' : t1,
           'run_statuses' : run_statuses,
           'invoke_statuses' : invoke_statuses}
    return res

def execute_local_stage(stage_function, tasks):
    stage_info = []
    for task in tasks:
        task['write_output'] = True
        stage_info.append(stage_function(task))
    res = {'results' : stage_info}
    return res

def execute_stage(stage_function, tasks):
    if mode == 'local':
       return execute_local_stage(stage_function, tasks)
    else:
        return execute_s3_stage(stage_function, tasks)


# In[ ]:





# In[5]:



# join
# mkdir_if_not_exist(output_address)
def stage1(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()
    output_address = key['output_address']
    cs = read_table(key)
    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()
    wanted_columns = ['ws_order_number',
                      'ws_ext_ship_cost',
                      'ws_net_profit',
                      'ws_ship_date_sk',
                      'ws_ship_addr_sk',
                      'ws_web_site_sk',
                      'ws_warehouse_sk']
    cs_s = cs[wanted_columns]

    t1 = time.time()
    tc += t1 - t0

    storage = output_address + "/part_" + str(key['task_id']) + "_"
    res = write_partitions(cs_s, ['ws_order_number'], 'uniform', parall_1, storage)
    outputs_info = res['outputs_info']
    [tcc, tww] = res['breakdown']
    tc += tcc
    tw += tww

    results = {}
    info = {}
    if 'write_output' in key and key['write_output']:
        info['outputs_info'] = outputs_info
    #results['info'] = {}
    results['info'] = info
    results['breakdown'] = [tr, tc, tw, (tc+tc+tw)]
    
    return results



# In[20]:


# mkdir_if_not_exist(output_address)

def stage2(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    cr = read_table(key)
    
    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    storage = output_address + "/part_" + str(key['task_id']) + "_"
    res = write_partitions(cr, ['wr_order_number'], 'uniform', parall_1, storage)
    outputs_info = res['outputs_info']
    [tcc, tww] = res['breakdown']
    tc += tcc
    tw += tww

    results = {}
    info = {}
    if 'write_output' in key and key['write_output']:
        info['outputs_info'] = outputs_info
    results['info'] = info
    #results['info'] = {}
    results['breakdown'] = [tr, tc, tw, (tc+tc+tw)]
    return results


# In[21]:


# mkdir_if_not_exist(output_address)
#@profile
def stage3(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    #print(key['names'])
    #print(key['dtypes'])
    cs = read_multiple_splits(key['names'], key['dtypes'], key['prefix'], key['number_splits'], key['suffix'])
    cr = read_multiple_splits(key['names2'], key['dtypes2'], key['prefix2'], key['number_splits2'], key['suffix2'])
    
    d = read_table(table)
    #return 1
    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    cs_succient = cs[['ws_order_number', 'ws_warehouse_sk']]
    '''
    cs_sj = pd.merge(cs, cs_succient, on=['cs_order_number'])
    del cs
    del cs_succient
    cs_sj_f1 = cs_sj[cs_sj.cs_warehouse_sk_x != cs_sj.cs_warehouse_sk_y]
    del cs_sj
    cs_sj_f1.drop_duplicates(subset=cs_sj_f1.columns[:-1], inplace=True)
    '''
    # the above impl eats too much memory
    # trying an alternative
    # 
    wh_uc = cs_succient.groupby(['ws_order_number']).agg({'ws_warehouse_sk':'nunique'})
    target_order_numbers = wh_uc.loc[wh_uc['ws_warehouse_sk'] > 1].index.values
    cs_sj_f1 = cs.loc[cs['ws_order_number'].isin(target_order_numbers)]

    cs_sj_f2 = cs_sj_f1.loc[cs_sj_f1['ws_order_number'].isin(cr.wr_order_number)]
    del cs_sj_f1
    #cs_sj_f2.rename(columns = {'cs_warehouse_sk_y':'cs_warehouse_sk'}, inplace = True)
    
     
    
    # join date_dim
    dd = d[['d_date', 'd_date_sk']]
    dd_select = dd[(pd.to_datetime(dd['d_date']) > pd.to_datetime('1999-02-01')) & (pd.to_datetime(dd['d_date']) < pd.to_datetime('1999-04-01'))]
    dd_filtered = dd_select[['d_date_sk']]
    
    merged = cs_sj_f2.merge(dd_filtered, left_on='ws_ship_date_sk', right_on='d_date_sk')
    del dd
    del cs_sj_f2
    del dd_select
    del dd_filtered
    merged.drop('d_date_sk', axis=1, inplace=True)
    
    # now partition with cs_ship_addr_sk
    storage = output_address + "/part_" + str(key['task_id']) + "_"
    
    t1 = time.time()
    tc += t1 - t0
    t0 = time.time()

    #print(merged.dtypes)
    res = write_partitions(merged, ['ws_ship_addr_sk'], 'uniform', parall_2, storage)
    outputs_info = res['outputs_info']
    [tcc, tww] = res['breakdown']
    tc += tcc
    tw += tww

    results = {}
    info = {}
    if 'write_output' in key and key['write_output']:
        info['outputs_info'] = outputs_info
    results['info'] = info
    #results['info'] = {}
    results['breakdown'] = [tr, tc, tw, (tc+tc+tw)]
    return results



# In[22]:


# mkdir_if_not_exist(output_address)
def stage4(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    cs = read_table(key)
    
    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    storage = output_address + "/part_" + str(key['task_id']) + "_"
    cs = cs[cs.ca_state == 'IL'][['ca_address_sk']]
    
    t1 = time.time()
    tc += t1 - t0
    t0 = time.time()

    res = write_partitions(cs, ['ca_address_sk'], 'uniform', parall_2, storage)
    outputs_info = res['outputs_info']
    [tcc, tww] = res['breakdown']
    tc += tcc
    tw += tww

    results = {}
    info = {}
    if 'write_output' in key and key['write_output']:
        info['outputs_info'] = outputs_info
    results['info'] = info
    #results['info'] = {}
    results['breakdown'] = [tr, tc, tw, (tc+tc+tw)]
    return results



# In[23]:


# mkdir_if_not_exist(output_address)
def stage5(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    cs = read_multiple_splits(key['names'], key['dtypes'], key['prefix'], key['number_splits'], key['suffix'])
    ca = read_multiple_splits(key['names2'], key['dtypes2'], key['prefix2'], key['number_splits2'], key['suffix2'])
    cc = read_table(key['web_site'])

    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    merged = cs.merge(ca, left_on='ws_ship_addr_sk', right_on='ca_address_sk')
    merged.drop('ws_ship_addr_sk', axis=1, inplace=True)
    
    #list_addr = ['Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County']
    cc_p = cc[cc['web_company_name'] == 'pri'][['web_site_sk']]
    
    #print(cc['cc_country'])
    merged2 = merged.merge(cc_p, left_on='ws_web_site_sk', right_on='web_site_sk')
    
    toshuffle = merged2[['ws_order_number', 'ws_ext_ship_cost', 'ws_net_profit']]
    
    storage = output_address + "/part_" + str(key['task_id']) + "_"
    
    t1 = time.time()
    tc += t1 - t0
    t0 = time.time()

    res = write_partitions(toshuffle, ['ws_order_number'], 'uniform', parall_3, storage)
    outputs_info = res['outputs_info']
    [tcc, tww] = res['breakdown']
    tc += tcc
    tw += tww

    results = {}
    info = {}
    if 'write_output' in key and key['write_output']:
        info['outputs_info'] = outputs_info
    results['info'] = info
    #results['info'] = {}
    results['breakdown'] = [tr, tc, tw, (tc+tc+tw)]
    return results



# In[24]:


# mkdir_if_not_exist(output_address)
def stage6(key):
    [tr, tc, tw] = [0] * 3
    t0 = time.time()

    output_address = key['output_address']
    cs = read_multiple_splits(key['names'], key['dtypes'], key['prefix'], key['number_splits'], key['suffix'])
    
    t1 = time.time()
    tr += t1 - t0
    t0 = time.time()

    a1 = pd.unique(cs['ws_order_number']).size
    a2 = cs['ws_ext_ship_cost'].sum()
    a3 = cs['ws_net_profit'].sum()
    
    t1 = time.time()
    tc += t1 - t0
    t0 = time.time()
    
    results = {}
    info = {}
    info['outputs_info'] = ''
    results['info'] = {}
    results['breakdown'] = [tr, tc, tw, (tc+tc+tw)]
    return results



# In[25]:

    


# In[6]:


results = []


# In[26]:


table = "web_sales"
names = get_name_for_table(table)
dtypes = get_dtypes_for_table(table)
tasks_stage1 = []
task_id = 0
all_locs = get_locations(table)
chunks = [all_locs[x:min(x+1,len(all_locs))] for x in xrange(0, len(all_locs), 1)]
for loc in chunks:
    key = {}
    # print(task_id)
    key['task_id'] = task_id
    task_id += 1
    key['loc'] = loc
    key['names'] = names
    key['dtypes'] = dtypes
    key['output_address'] = temp_address + "intermediate/stage1"
    tasks_stage1.append(key)
    
#stage1_info = []
#for task in tasks_stage1:
#    stage1_info.append(stage1(task))

#results_stage = execute_stage(stage1, [tasks_stage1[0]])
#'''
#results_stage = execute_stage(stage1, tasks_stage1)
results_stage = execute_local_stage(stage1, [tasks_stage1[0]])
#results_stage = execute_stage(stage1, [tasks_stage1[0]])
stage1_info = [a['info'] for a in results_stage['results']]
#print(stage1_info)
stage_info_load['1'] = stage1_info[0]
#exit(0)
results.append(results_stage)

#print(results_stage)
pickle.dump(results, open(filename, 'wb'))
#'''
#exit(0)
# In[27]:


# execute_stage(stage1, [tasks_stage1[0]])


# In[28]:


table = "web_returns"
names = get_name_for_table(table)
dtypes = get_dtypes_for_table(table)
tasks_stage2 = []
task_id = 0
all_locs = get_locations(table)
chunks = [all_locs[x:min(x+1,len(all_locs))] for x in xrange(0, len(all_locs), 1)]
for loc in chunks:
    key = {}
    # print(task_id)
    key['task_id'] = task_id
    task_id += 1
    key['loc'] = loc
    key['names'] = names
    key['dtypes'] = dtypes
    key['output_address'] = temp_address + "intermediate/stage2"

    tasks_stage2.append(key)
    

#results_stage = execute_stage(stage2, tasks_stage2)
results_stage = execute_local_stage(stage2, [tasks_stage2[0]])
stage2_info = [a['info'] for a in results_stage['results']]
results.append(results_stage)
stage_info_load['2'] = stage2_info[0]

pickle.dump(results, open(filename, 'wb'))

#exit(0)
# In[29]:


# print(stage1_info)


# In[30]:

#exit(0)

tasks_stage3 = []
task_id = 0
date_dim_loc = get_locations("date_dim")[0]
for i in range(parall_1):
    #print(info)
    info = stage_info_load['1']
    info2 = stage_info_load['2']
    key = {}
    key['task_id'] = task_id
    key['prefix'] = temp_address + "intermediate/stage1/part_"
    key['suffix'] = "_" + str(task_id) + ".csv"
    key['names'] = info["outputs_info"][0]['names']
    key['dtypes'] = info["outputs_info"][0]['dtypes']
    key['number_splits'] = len(tasks_stage1)
    
    key['prefix2'] = temp_address + "intermediate/stage2/part_"
    key['suffix2'] = "_" + str(task_id) + ".csv"
    key['names2'] = info2["outputs_info"][0]['names']
    key['dtypes2'] = info2["outputs_info"][0]['dtypes']
    key['number_splits2'] = len(tasks_stage2)
    
    table = {}
    table['names'] = get_name_for_table("date_dim")
    table['dtypes'] = get_dtypes_for_table("date_dim")
    table['loc'] = date_dim_loc
    key['date_dim'] = table
    
    key['output_address'] = temp_address + "intermediate/stage3"


    tasks_stage3.append(key)
    task_id += 1


#results_stage = execute_stage(stage3, tasks_stage3)
results_stage = execute_local_stage(stage3, [tasks_stage3[0]])
#print(results_stage)
stage3_info = [a['info'] for a in results_stage['results']]
results.append(results_stage)
stage_info_load['3'] = stage3_info[0]

exit(0)
pickle.dump(results, open(filename, 'wb'))
#exit(0)

# In[ ]:





# In[31]:


table = "customer_address"
names = get_name_for_table(table)
dtypes = get_dtypes_for_table(table)
tasks_stage4 = []
task_id = 0
for loc in get_locations(table):
    key = {}
    # print(task_id)
    key['task_id'] = task_id
    task_id += 1
    key['loc'] = loc
    key['names'] = names
    key['dtypes'] = dtypes
    key['output_address'] = temp_address + "intermediate/stage4"
        
    tasks_stage4.append(key)
#'''    
results_stage = execute_local_stage(stage4, [tasks_stage4[0]])
#results_stage = execute_stage(stage4, tasks_stage4)
stage4_info = [a['info'] for a in results_stage['results']]
results.append(results_stage)
stage_info_load['4'] = stage4_info[0]


pickle.dump(results, open(filename, 'wb'))
#'''

# In[32]:



tasks_stage5 = []
task_id = 0
call_center_loc = get_locations("web_site")[0]
#print(info["outputs_info"][0])
for i in range(parall_2):
    key = {}
    info = stage_info_load['3']
    info2 = stage_info_load['4']
    key = {}
    key['task_id'] = task_id
    
    key['prefix'] = temp_address + "intermediate/stage3/part_"
    key['suffix'] = "_" + str(task_id) + ".csv"
    key['names'] = info["outputs_info"][0]['names']
    key['dtypes'] = info["outputs_info"][0]['dtypes']
    key['number_splits'] = len(tasks_stage3)
    
    key['prefix2'] = temp_address + "intermediate/stage4/part_"
    key['suffix2'] = "_" + str(task_id) + ".csv"
    key['names2'] = info2["outputs_info"][0]['names']
    key['dtypes2'] = info2["outputs_info"][0]['dtypes']
    key['number_splits2'] = len(tasks_stage4)
    
    table = {}
    table['names'] = get_name_for_table("web_site")
    table['dtypes'] = get_dtypes_for_table("web_site")
    table['loc'] = call_center_loc
    key['web_site'] = table
    
    key['output_address'] = temp_address + "intermediate/stage5"

    tasks_stage5.append(key)
    task_id += 1
    
#results_stage = execute_stage(stage5, tasks_stage5)
results_stage = execute_local_stage(stage5, [tasks_stage5[0]])
stage5_info = [a['info'] for a in results_stage['results']]
stage_info_load['5'] = stage5_info[0]
pickle.dump(stage_info_load, open("stage_info_load_94.pickle", "wb"))

results.append(results_stage)

pickle.dump(results, open(filename, 'wb'))
#exit(0)

# In[33]:


tasks_stage6 = []
task_id = 0
for i in range(parall_3):
    key = {}
    info = stage_info_load['5']
    key = {}
    key['task_id'] = task_id
    
    key['prefix'] = temp_address + "intermediate/stage5/part_"
    key['suffix'] = "_" + str(task_id) + ".csv"
    key['names'] = info["outputs_info"][0]['names']
    key['dtypes'] = info["outputs_info"][0]['dtypes']
    key['number_splits'] = len(tasks_stage5)
        
    key['output_address'] = temp_address + "intermediate/stage6"
            
    tasks_stage6.append(key)
    task_id += 1
    
results_stage = execute_stage(stage6, tasks_stage6)
#results_stage = execute_local_stage(stage6, [tasks_stage6[0]])
stage6_info = [a['info'] for a in results_stage['results']]
results.append(results_stage)


# In[34]:


pickle.dump(results, open(filename, 'wb'))


# In[35]:


# print(results)


