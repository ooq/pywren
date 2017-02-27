#from gevent import monkey

#monkey.patch_socket()
#monkey.patch_ssl()

import pywren
import boto3
import md5

if __name__ == "__main__":
    import logging
    import subprocess
    #logging.basicConfig(level=logging.DEBUG)

    # fh = logging.FileHandler('simpletest.log')
    # fh.setLevel(logging.DEBUG)
    # fh.setFormatter(pywren.wren.formatter)
    # pywren.wren.logger.addHandler(fh)


    def run_command(key):
        pywren.wrenlogging.default_config()
        logger = logging.getLogger(__name__)


        client = boto3.client('s3', 'us-west-2')
        client.download_file('qifan-public', 'gensort', '/tmp/condaruntime/gensort')

        for i in range(0,1):
            number_of_records = 1000 * 1000
            begin = key * number_of_records

            res0 = subprocess.check_output(["rm", "-rf", "/tmp/condaruntime/input"])
            res1 = subprocess.check_output(["mkdir", "-p", "/tmp/condaruntime/input"])
            res2 = subprocess.check_output(["chmod", "a+x", "/tmp/condaruntime/gensort"])
            res4 = subprocess.check_output(["df", "-h"])

            logger.info(res4)
            print res4
            print 'generating'
            # res3 = subprocess.check_output(["/tmp/condaruntime/gensort",
            #                                 "-b"+str(begin),
            #                                 str(number_of_records),
            #                                 "/tmp/condaruntime/input/part-" + str(key)])
            res4 = subprocess.check_output(["ls", "-lh", "/tmp/condaruntime"])
            logger.info(res4)
            print res4
            keyname = "/input/part-" + str(key)
            m = md5.new()
            m.update(keyname)
            randomized_keyname = m.hexdigest()[:8] + keyname
            #client.upload_file("/tmp/condaruntime/input/part-"+str(key), "sort-data-random-1t", randomized_keyname)
            key = key+1

    wrenexec = pywren.default_executor()
    fut = wrenexec.map(run_command, range(0,10,10))

    pywren.wait(fut)
    res = [f.result() for f in fut]
    print res