from pywren import s3util
import boto3
import os
import time


def get_callset_done(bucket, prefix, callset_id):
    key_prefix = os.path.join(prefix, callset_id)
    s3 = boto3.resource('s3', region_name="us-west-2")
    tstart = time.time()
    tend = time.time()
    print "start"
    s3res = s3.meta.client.list_objects_v2(Bucket=bucket, Prefix=key_prefix, 
                                           MaxKeys=1000)
    
    status_keys = []
    succeeded_keys_keys = []
    
    objects = 0
    while True:
        objects += 1000
        print str(objects) + " time spent " + str(time.time()-tend) + " total " + str(time.time()-tstart)
        
        tend = time.time()
        
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


bucket = "pywren-test-1"
prefix = "pywren.jobs"
callset_id = "c4a1ca55-4735-4388-9706-5d51f4530096"
callset_id = "109be841-2fe3-489e-b1b5-c38e8857228f"

succeeded_call_ids, other_calls_attempts = get_callset_done(bucket, prefix, callset_id)

for id in succeeded_call_ids:
    print id
#print len(succeeded_call_ids)
#print len(other_calls_attempts)
