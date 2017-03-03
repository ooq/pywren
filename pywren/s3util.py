import boto3
import os
import botocore
import sys

if (sys.version_info > (3, 0)):
    from . import wrenutil
    from . import wrenconfig
else:
    import wrenutil
    import wrenconfig


def create_callset_id():
    return wrenutil.uuid_str()

def create_call_id():
    return wrenutil.uuid_str()

def create_keys(bucket, prefix, callset_id, call_id):
    data_key = (bucket, os.path.join(prefix, callset_id, call_id, "data.pickle"))
    output_key = (bucket, os.path.join(prefix, callset_id, call_id, "output.pickle"))
    status_key_prefix = (bucket, os.path.join(prefix, callset_id, call_id, "status"))
    return data_key, output_key, status_key_prefix

def create_func_key(bucket, prefix, callset_id):
    func_key = (bucket, os.path.join(prefix, callset_id, "func.json"))
    return func_key

def create_agg_data_key(bucket, prefix, callset_id):
    func_key = (bucket, os.path.join(prefix, callset_id, "aggdata.pickle"))
    return func_key


def key_size(bucket, key):
    try:

        s3 = boto3.resource('s3')
        a = s3.meta.client.head_object(Bucket=bucket, Key=key)
        return a['ContentLength']
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return None
        else:
            raise e


def get_callset_done(bucket, prefix, callset_id):
    key_prefix = os.path.join(prefix, callset_id)
    s3 = boto3.resource('s3', region_name=wrenconfig.AWS_REGION)
    s3res = s3.meta.client.list_objects_v2(Bucket=bucket, Prefix=key_prefix, 
                                           MaxKeys=1000)
    
    status_keys = []
    succeeded_keys_keys = []

    while True:
        for k in s3res['Contents']:
            if "status.json" in k['Key']:
                status_keys.append(k['Key'])
            if "status-success.json" in k['Key']:
                succeeded_keys_keys.append(k['Key'])

        if 'NextContinuationToken' in s3res:
            continuation_token = s3res['NextContinuationToken']
            s3res = s3.meta.client.list_objects_v2(Bucket=bucket, Prefix=key_prefix, 
                                                   MaxKeys=1000, 
                                                   ContinuationToken = continuation_token)
        else:
            break

    succeeded_call_ids = set([k[len(key_prefix)+1:].split("/")[0] for k in succeeded_keys_keys])
    other_calls_attempts = {}
    for k in status_keys:
        call_id = k[len(key_prefix)+1:].split("/")[0]
        if call_id not in succeeded_call_ids:
            if call_id not in other_calls_attempts:
                other_calls_attempts[call_id] = 1
            else:
                other_calls_attempts[call_id] += 1
    return succeeded_call_ids, other_calls_attempts
