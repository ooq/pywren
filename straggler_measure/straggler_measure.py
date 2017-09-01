import pywren
import time
import sys
import cPickle as pickle

wrenexec = pywren.default_executor(shard_runtime=True)

def stage_function(key):
   time.sleep(20) 

tasks = range(500)
pywren_rate = len(tasks)
deal_straggler = False
futures = wrenexec.map_sync_with_rate_and_retries(stage_function, tasks, straggler=deal_straggler, WAIT_DUR_SEC=1, rate=pywren_rate)
    
results = [f.result() for f in futures]
run_statuses = [f.run_status for f in futures]
invoke_statuses = [f.invoke_status for f in futures]
res = {'results' : results,
       'run_statuses' : run_statuses,
       'invoke_statuses' : invoke_statuses}


if (len(sys.argv) < 2):
    filename = "straggler.pickle"
else:
    filename = sys.argv[1]

pickle.dump(res, open(filename, 'wb'))

