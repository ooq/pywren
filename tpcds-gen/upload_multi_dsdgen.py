import boto3
import md5
import numpy as np
import random

for genpart in range(50):
        print(genpart)
        m = md5.new()
        m.update(str(genpart))
        genfile = 'tools/' + m.hexdigest()[:8] + "-dsdgen-" + str(genpart)
        idxfile = 'tools/' + m.hexdigest()[:8] + "-idx-" + str(genpart)
        client = boto3.client('s3', 'us-west-2')
        client.upload_file('/home/ec2-user/dsdgen', 'qifan-public', genfile)
        client.upload_file('/home/ec2-user/tpcds.idx', 'qifan-public', idxfile)
