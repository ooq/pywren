from pywren import s3util
import sys
import os
import boto3
cs=sys.argv[1]
bucket="pywren-test-1"
prefix="pywren.jobs"
l1,l2=s3util.get_callset_done(bucket,prefix,cs)
full=["{:05d}".format(i) for i in range(2500)]
full=set(full)
diffs = full.difference(l1)
s3client = boto3.client("s3", "us-west-2")
for diff in diffs:
    newprefix=os.path.join(prefix,cs,diff)
    s3res = s3client.list_objects_v2(Bucket=bucket, Prefix=newprefix, MaxKeys=1000)
    print("searching for " + diff)
    if 'Contents' in s3res:
        for k in s3res['Contents']:
            if "status.json" in k['Key']:
                print("found status for " + diff)
            if "status-success.json" in k['Key']: 
                print("found success for " + diff)
    else:
        print(s3res)
