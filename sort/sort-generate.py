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

        client = boto3.client('s3', 'us-west-2')

        res1 = subprocess.check_output(["ls", "-lh", "/tmp/condaruntime/"])
        print "res1 ", res1
        res2 = subprocess.check_output(["/tmp/condaruntime/gensort", "-b10", "10",
                                        "/tmp/condaruntime/part1"])
        print "res2 ", res2
        res3 = subprocess.check_output(["ls", "-lh", "/tmp/condaruntime/"])
        print "res3 ", res3
        client.upload_file("/tmp/condaruntime/part1", "sort-data", "input1/part1")

            

    wrenexec = pywren.default_executor()
    fut = wrenexec.map(run_command, [1])

    res = [f.result() for f in fut]
    print res

