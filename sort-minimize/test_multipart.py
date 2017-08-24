import boto3

bucket = 'qifan-test-bucket'
key = 'mp-test'

s3 = boto3.client('s3')

# Initiate the multipart upload and send the part(s)
mpu = s3.create_multipart_upload(Bucket=bucket, Key=key)
part1 = s3.upload_part(Bucket=bucket, Key=key, PartNumber=1,
                       UploadId=mpu['UploadId'], Body='Hello, world!')
part2 = s3.upload_part(Bucket=bucket, Key=key, PartNumber=2,
                       UploadId=mpu['UploadId'], Body='Hello, world!')

# Next, we need to gather information about each part to complete
# the upload. Needed are the part number and ETag.
part_info = {
    'Parts': [
        {
            'PartNumber': 1,
            'ETag': part1['ETag']
        },
        {
            'PartNumber': 2,
            'ETag': part2['ETag']
        }
    ]
}

# Now the upload works!
s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=mpu['UploadId'],
                             MultipartUpload=part_info)

data = s3.get_object(Bucket=bucket, Key=key)
data_read = data['Body'].read()
print data_read.size