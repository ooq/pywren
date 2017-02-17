#from gevent import monkey

#monkey.patch_socket()
#monkey.patch_ssl()

import pywren
import boto3

if __name__ == "__main__":
    import logging
    import subprocess
    #logging.basicConfig(level=logging.DEBUG)

    # fh = logging.FileHandler('simpletest.log')
    # fh.setLevel(logging.DEBUG)
    # fh.setFormatter(pywren.wren.formatter)
    # pywren.wren.logger.addHandler(fh)


    def run_command(key):
        logger = logging.getLogger(__name__)

        for i in range(0,20):
            number_of_records = 1000 * 1000
            begin = key * number_of_records

            client = boto3.client('s3', 'us-west-2')

            res0 = subprocess.check_output(["rm", "-rf", "/tmp/condaruntime/input"])
            res1 = subprocess.check_output(["mkdir", "-p", "/tmp/condaruntime/input"])
            res2 = subprocess.check_output(["/tmp/condaruntime/gensort",
                                            "-b"+str(begin),
                                            str(number_of_records),
                                            "/tmp/condaruntime/input/part-" + str(key)])
            client.upload_file("/tmp/condaruntime/input/part-"+str(key), "sort-data", "input/part-" + str(key))
            key = key+1

    wrenexec = pywren.default_executor()
    fut = wrenexec.map(run_command, range(1,10000,20))

    res = [f.result() for f in fut]
    print res

