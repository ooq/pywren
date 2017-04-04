#from gevent import monkey

#monkey.patch_socket()
#monkey.patch_ssl()

import pywren
import time
import botocore
import boto3

if __name__ == "__main__":
    import logging
    #logging.basicConfig(level=logging.DEBUG)

    # fh = logging.FileHandler('simpletest.log')
    # fh.setLevel(logging.DEBUG)
    # fh.setFormatter(pywren.wren.formatter)
    # pywren.wren.logger.addHandler(fh)

    def test_add(x):
        time.sleep(0)
        return x

    wrenexec = pywren.default_executor(shard_runtime=True)
    futures = wrenexec.map(test_add, range(1000))
    pywren.wait(futures)
    results = [f.result() for f in futures]
    print(results)
    run_statuses = [f.run_status['download_time'] for f in futures]
    print(run_statuses)
    print ""
    print("all " + str(sum(run_statuses)))
    #cached1 = [f.run_status['runtime_cached'] for f in futures if not f.run_status['runtime_cached']]
    #cached2 = [f.run_status['runtime_cached'] for f in futures if f.run_status['runtime_cached']]
    #print cached1
    #print cached2
    cached = [f for f in futures if f.run_status['runtime_cached']]
    print("num cached " + str(len(cached)))
    run_statuses_new = [f.run_status['download_time'] for f in futures if (not f.run_status['runtime_cached'])]
    print("uncached " + str(sum(run_statuses_new)))

