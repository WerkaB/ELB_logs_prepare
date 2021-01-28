import boto3
import os


def download_and_unzip():
    bucket_name = os.getenv("bucket_name")
    destination_directory = os.getenv("destination_directory")

    s3_resource = boto3.resource('s3')
    s3_client = boto3.client('s3')
    my_bucket = s3_resource.Bucket(bucket_name)

    for my_bucket_object in my_bucket.objects.filter(Prefix='AWSLogs/181550855204/elasticloadbalancing/eu-central-1/2021/01/28'):
        print(my_bucket_object)
        path, filename = os.path.split(my_bucket_object.key)
        absolute_path = os.path.join(destination_directory,filename)
        my_bucket.download_file(my_bucket_object.key, absolute_path)
        # s3_client.download_file()
        pass
    pass