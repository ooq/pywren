from pywren import s3util

bucket = "pywren-test-1"
prefix = "pywren.jobs"
callset_id = "c4a1ca55-4735-4388-9706-5d51f4530096"
succeeded_call_ids, other_calls_attempts = s3util.get_callset_done(bucket, prefix, callset_id)

print len(succeeded_call_ids)
print (other_calls_attempts)