import pywren
import boto3
import md5

if __name__ == "__main__":
    import logging
    import subprocess
    import gc
    import time
    def run_command(key):
        pywren.wrenlogging.default_config()
        logger = logging.getLogger(__name__)


        client = boto3.client('s3', 'us-west-2')
        client.download_file('qifan-public', 'gensort', '/tmp/condaruntime/gensort')
        res = subprocess.check_output(["chmod",
                                        "a+x",
                                        "/tmp/condaruntime/gensort"])

        for i in range(0,20):
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
            client.put_object(Body = data, Bucket = "sort-data-random-1t", Key = randomized_keyname)
            logger.info(str(key) + "th object uploaded.")
            gc_start = time.time()
            gc.collect()
            gc_end = time.time()
            logger.info("GC takes " + str(gc_end - gc_start) + " seconds.")
            key = key + 1

    wrenexec = pywren.default_executor()
    fut = wrenexec.map(run_command, range(0,10000,20))

    pywren.wait(fut)
    res = [f.result() for f in fut]
    print res