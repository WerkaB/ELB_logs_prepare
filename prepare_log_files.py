import boto3
import os
import gzip
from os import walk
import shutil


def download_and_unzip():
    bucket_name = os.getenv("bucket_name")
    download_directory = os.getenv("download_directory")

    s3_resource = boto3.resource('s3')
    s3_client = boto3.client('s3')
    my_bucket = s3_resource.Bucket(bucket_name)

    for my_bucket_object in my_bucket.objects.filter(Prefix='AWSLogs/181550855204/elasticloadbalancing/eu-central-1/2021/01/28'):
        print(my_bucket_object)
        path, filename = os.path.split(my_bucket_object.key)
        absolute_path = os.path.join(download_directory,filename)
        my_bucket.download_file(my_bucket_object.key, absolute_path)
        # s3_client.download_file()

    unzip_files(download_directory)


def unzip_files(download_directory: str):
    destination_unzipped_directory = os.path.join(download_directory,"unzipped")

    _, _, filenames = next(walk(download_directory))

    for filename in filenames:
        abs_path_file_in = os.path.join(download_directory,filename)
        abs_path_file_out = os.path.join(destination_unzipped_directory,filename)[:-3]

        with gzip.open(abs_path_file_in, 'rb') as f_in:
            with open(abs_path_file_out, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
